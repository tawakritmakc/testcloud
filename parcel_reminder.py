"""
ParcelReminder - Lambda เตือนซ้ำเมื่อพัสดุค้างนานเกินกำหนด (CS615)
เรียกโดย EventBridge ทุกวัน 09:00 น. (cron(0 2 * * ? *) = 02:00 UTC)

Environment variables:
  PARCELS_TABLE, TOPIC_ARN, REMIND_AFTER_HOURS (ค่าเริ่มต้น 72)
"""
import os
from datetime import datetime, timedelta, timezone

import boto3
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

parcels_table = dynamodb.Table(os.environ["PARCELS_TABLE"])
TOPIC_ARN = os.environ["TOPIC_ARN"]
REMIND_AFTER_HOURS = float(os.environ.get("REMIND_AFTER_HOURS", "72"))
MIN_HOURS_BETWEEN_REMINDERS = 20  # กันส่งเตือนพัสดุเดิมซ้ำในวันเดียวกัน

THAI_TZ = timezone(timedelta(hours=7))


def thai_time(iso_str):
    dt = datetime.fromisoformat(iso_str).astimezone(THAI_TZ)
    return dt.strftime("%d/%m/") + str(dt.year + 543) + dt.strftime(" %H:%M น.")


def hours_since(iso_str, now):
    return (now - datetime.fromisoformat(iso_str)).total_seconds() / 3600


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)

    items, kwargs = [], {"FilterExpression": Attr("status").eq("WAITING")}
    while True:
        resp = parcels_table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    sent = 0
    for p in items:
        waited = hours_since(p["createdAt"], now)
        if waited < REMIND_AFTER_HOURS:
            continue
        last = p.get("lastRemindedAt")
        if last and hours_since(last, now) < MIN_HOURS_BETWEEN_REMINDERS:
            continue

        days = int(waited // 24)
        message = (
            f"เตือน: ห้อง {p['room']} ยังมีพัสดุรอรับที่หอพัก\n\n"
            f"มาถึงเมื่อ: {thai_time(p['createdAt'])} (ค้างมา {days} วัน)\n"
            f"ขนส่ง: {p.get('carrier', '-')}\n"
            f"รูปพัสดุ: {p.get('photoUrl') or '-'}\n\n"
            f"รหัสรับพัสดุ: {p['pickupCode']}\n"
            f"กรุณามารับพัสดุโดยเร็ว"
        )
        sns.publish(
            TopicArn=TOPIC_ARN,
            Subject=f"เตือน: พัสดุห้อง {p['room']} ยังไม่มีคนมารับ",
            Message=message,
            MessageAttributes={"room": {"DataType": "String", "StringValue": p["room"]}},
        )
        parcels_table.update_item(
            Key={"parcelId": p["parcelId"]},
            UpdateExpression="SET remindCount = if_not_exists(remindCount, :zero) + :one, lastRemindedAt = :now",
            ExpressionAttributeValues={":zero": 0, ":one": 1, ":now": now.isoformat()},
        )
        sent += 1

    print(f"Checked {len(items)} waiting parcels, sent {sent} reminders")
    return {"waiting": len(items), "reminded": sent}
