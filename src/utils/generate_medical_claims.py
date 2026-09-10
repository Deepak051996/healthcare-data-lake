import csv
import random
from datetime import date, timedelta
from pathlib import Path


OUTPUT_FILE = Path("data/source/medical_claims.csv")


def generate_medical_claims():
    claims = []

    claim_statuses = [
        "Submitted",
        "Approved",
        "Rejected",
        "Pending"
    ]

    claim_types = [
        "Inpatient",
        "Outpatient",
        "Pharmacy",
        "Diagnostic",
        "Emergency"
    ]

    start_date = date(2025, 1, 1)
    end_date = date(2026, 12, 31)

    # Load provider -> hospital mapping
    provider_hospital = {}

    with open("data/source/providers.csv", "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            provider_hospital[row["provider_id"]] = row["hospital_id"]

    patient_ids = [
        f"P{i:05d}"
        for i in range(1, 10001)
    ]

    provider_ids = list(provider_hospital.keys())

    for i in range(1, 30001):

        patient_id = random.choice(patient_ids)
        provider_id = random.choice(provider_ids)

        # Hospital is derived from the selected provider
        hospital_id = provider_hospital[provider_id]

        random_days = random.randint(
            0,
            (end_date - start_date).days
        )

        claim_date = start_date + timedelta(
            days=random_days
        )

        claim = {
            "claim_id": f"C{i:06d}",
            "patient_id": patient_id,
            "provider_id": provider_id,
            "hospital_id": hospital_id,
            "claim_date": claim_date.isoformat(),
            "claim_amount": round(random.uniform(500, 500000), 2),
            "claim_status": random.choice(claim_statuses),
            "claim_type": random.choice(claim_types)
        }

        claims.append(claim)

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        OUTPUT_FILE,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=[
                "claim_id",
                "patient_id",
                "provider_id",
                "hospital_id",
                "claim_date",
                "claim_amount",
                "claim_status",
                "claim_type"
            ]
        )

        writer.writeheader()
        writer.writerows(claims)

    print(f"Generated {len(claims)} medical claim records.")
    print(f"Output file: {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_medical_claims()