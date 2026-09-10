import csv
import random
from datetime import date, timedelta
from pathlib import Path


OUTPUT_FILE = Path("data/source/appointments.csv")


def generate_appointments():
    appointments = []

    appointment_statuses = [
        "Scheduled",
        "Completed",
        "Cancelled",
        "No Show"
    ]

    diagnoses = [
        "Hypertension",
        "Diabetes",
        "Common Cold",
        "Migraine",
        "Asthma",
        "Arthritis",
        "Heart Disease",
        "Back Pain",
        "Fever",
        "Routine Checkup"
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

    for i in range(1, 50001):

        patient_id = random.choice(patient_ids)
        provider_id = random.choice(provider_ids)

        # Important:
        # Hospital comes from the selected provider.
        hospital_id = provider_hospital[provider_id]

        random_days = random.randint(
            0,
            (end_date - start_date).days
        )

        appointment_date = start_date + timedelta(
            days=random_days
        )

        appointment = {
            "appointment_id": f"A{i:06d}",
            "patient_id": patient_id,
            "provider_id": provider_id,
            "hospital_id": hospital_id,
            "appointment_date": appointment_date.isoformat(),
            "appointment_status": random.choice(appointment_statuses),
            "diagnosis": random.choice(diagnoses)
        }

        appointments.append(appointment)

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
                "appointment_id",
                "patient_id",
                "provider_id",
                "hospital_id",
                "appointment_date",
                "appointment_status",
                "diagnosis"
            ]
        )

        writer.writeheader()
        writer.writerows(appointments)

    print(f"Generated {len(appointments)} appointment records.")
    print(f"Output file: {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_appointments()