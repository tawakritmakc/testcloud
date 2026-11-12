"""
ParcelAPI - Lambda สำหรับระบบแจ้งเตือนพัสดุถึงหอพัก (CS615)

Routes (API Gateway HTTP API, payload format 2.0):
  GET  /residents              -> รายการห้องสำหรับเลือก
  POST /parcels                -> แจ้งพัสดุใหม่ พร้อมรูป (base64)
  GET  /parcels                -> รายการพัสดุที่ยังไม่มีคนรับ
  POST /parcels/{id}/pickup    -> ยืนยันการรับด้วยรหัส 4 หลัก

Environment variables:
  RESIDENTS_TABLE, PARCELS_TABLE, PHOTO_BUCKET, TOPIC_ARN
"""
import base64
import json
import os
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
sns = boto3.client("sns")

residents_table = dynamodb.Table(os.environ["RESIDENTS_TABLE"])
parcels_table = dynamodb.Table(os.environ["PARCELS_TABLE"])
PHOTO_BUCKET = os.environ["PHOTO_BUCKET"]
TOPIC_ARN = os.environ["TOPIC_ARN"]

THAI_TZ = timezone(timedelta(hours=7))


# ---------- helpers ----------
def respond(status, body):
    def default(o):
        if isinstance(o, Decimal):
            return int(o) if o % 1 == 0 else float(o)
        raise TypeError
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json; charset=utf-8"},
        "body": json.dumps(body, ensure_ascii=False, default=default),
    }


def parse_body(event):
    body = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return None


def thai_time(iso_str):
    dt = datetime.fromisoformat(iso_str).astimezone(THAI_TZ)
    return dt.strftime("%d/%m/") + str(dt.year + 543) + dt.strftime(" %H:%M น.")


def notify(room, subject, message):
    sns.publish(
        TopicArn=TOPIC_ARN,
        Subject=subject,
        Message=message,
        MessageAttributes={"room": {"DataType": "String", "StringValue": room}},
    )


# ---------- routes ----------
def list_residents():
    items = residents_table.scan().get("Items", [])
    # ไม่ส่งอีเมลออกไปที่หน้าเว็บ
    rooms = [{"room": i["room"], "name": i.get("name", "")} for i in items]
    rooms.sort(key=lambda r: r["room"])
    return respond(200, rooms)


def create_parcel(event):
    data = parse_body(event)
    if data is None:
        return respond(400, {"message": "Invalid JSON"})

    room = (data.get("room") or "").strip()
    carrier = (data.get("carrier") or "ไม่ระบุ").strip()
    photo = data.get("photo") or ""

    if not room:
        return respond(400, {"message": "room is required"})
    resident = residents_table.get_item(Key={"room": room}).get("Item")
    if not resident:
        return respond(404, {"message": f"Room {room} not found"})

    parcel_id = uuid.uuid4().hex[:12]
    pickup_code = f"{secrets.randbelow(10000):04d}"
    now = datetime.now(timezone.utc).isoformat()

    # บันทึกรูปลง S3 (รับได้ทั้ง data URL และ base64 ล้วน)
    photo_url = ""
    if photo:
        if "," in photo:
            photo = photo.split(",", 1)[1]
        try:
            image_bytes = base64.b64decode(photo)
        except Exception:
            return respond(400, {"message": "Invalid photo data"})
        key = f"photos/{parcel_id}-{secrets.token_hex(8)}.jpg"
        s3.put_object(Bucket=PHOTO_BUCKET, Key=key, Body=image_bytes, ContentType="image/jpeg")
        photo_url = f"https://{PHOTO_BUCKET}.s3.amazonaws.com/{key}"

    item = {
        "parcelId": parcel_id,
        "room": room,
        "carrier": carrier,
        "photoUrl": photo_url,
        "pickupCode": pickup_code,
        "status": "WAITING",
        "createdAt": now,
        "remindCount": 0,
    }
    parcels_table.put_item(Item=item)

    message = (
        f"เรียน คุณ{resident.get('name', '')} (ห้อง {room})\n\n"
        f"มีพัสดุมาถึงหอพักเมื่อ {thai_time(now)}\n"
        f"ขนส่ง: {carrier}\n"
        f"รูปพัสดุ: {photo_url or '-'}\n\n"
        f"รหัสรับพัสดุของคุณคือ: {pickup_code}\n"
        f"กรุณาแจ้งรหัสนี้กับเจ้าหน้าที่เมื่อมารับพัสดุ"
    )
    notify(room, f"มีพัสดุถึงห้อง {room}", message)

    result = {k: v for k, v in item.items() if k != "pickupCode"}  # ไม่เปิดเผยรหัสให้หน้าเว็บ
    return respond(201, {"message": "Parcel registered", "parcel": result})


def list_waiting_parcels():
    items, kwargs = [], {"FilterExpression": Attr("status").eq("WAITING")}
    while True:
        resp = parcels_table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    for i in items:
        i.pop("pickupCode", None)
    items.sort(key=lambda i: i["createdAt"], reverse=True)
    return respond(200, items)


def pickup_parcel(event):
    parcel_id = (event.get("pathParameters") or {}).get("id")
    data = parse_body(event)
    if data is None:
        return respond(400, {"message": "Invalid JSON"})
    code = str(data.get("code") or "").strip()

    parcel = parcels_table.get_item(Key={"parcelId": parcel_id}).get("Item")
    if not parcel:
        return respond(404, {"message": "Parcel not found"})
    if parcel["status"] == "PICKED_UP":
        return respond(409, {"message": "Parcel already picked up"})
    if code != parcel["pickupCode"]:
        return respond(403, {"message": "Invalid pickup code"})

    now = datetime.now(timezone.utc).isoformat()
    try:
        parcels_table.update_item(
            Key={"parcelId": parcel_id},
            UpdateExpression="SET #s = :picked, pickedUpAt = :now",
            ConditionExpression="#s = :waiting AND pickupCode = :code",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":picked": "PICKED_UP", ":waiting": "WAITING", ":code": code, ":now": now,
            },
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return respond(409, {"message": "Parcel already picked up"})
        raise

    notify(
        parcel["room"],
        f"รับพัสดุห้อง {parcel['room']} เรียบร้อย",
        f"พัสดุ ({parcel.get('carrier', '')}) ของห้อง {parcel['room']} "
        f"ถูกรับไปแล้วเมื่อ {thai_time(now)}\n"
        f"หากคุณไม่ได้มารับด้วยตนเอง กรุณาติดต่อเจ้าหน้าที่หอพักทันที",
    )
    return respond(200, {"message": "Parcel picked up", "parcelId": parcel_id})


# ---------- entry point ----------
def lambda_handler(event, context):
    route = event.get("routeKey", "")
    try:
        if route == "GET /residents":
            return list_residents()
        if route == "POST /parcels":
            return create_parcel(event)
        if route == "GET /parcels":
            return list_waiting_parcels()
        if route == "POST /parcels/{id}/pickup":
            return pickup_parcel(event)
        return respond(404, {"message": f"Route not found: {route}"})
    except Exception as e:
        print("ERROR:", repr(e))
        return respond(500, {"message": "Internal error", "detail": str(e)})
