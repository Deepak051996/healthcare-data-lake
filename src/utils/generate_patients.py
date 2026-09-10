import csv
import random
from datetime import date, timedelta
from pathlib import Path


OUTPUT_FILE = Path("data/source/patients.csv")


def generate_patients():
    patients = []

    first_names = [
        "Amit", "Rahul", "Priya", "Neha", "Rohit",
        "Anita", "Vikas", "Pooja", "Arjun", "Sneha"
    ]

    last_names = [
        "Sharma", "Kumar", "Singh", "Patel", "Gupta",
        "Verma", "Mehta", "Joshi", "Yadav", "Mishra"
    ]

    genders = ["M", "F"]

    locations = [
        ("Mumbai", "Maharashtra"),
        ("Pune", "Maharashtra"),
        ("Bengaluru", "Karnataka"),
        ("Delhi", "Delhi"),
        ("Chennai", "Tamil Nadu"),
        ("Hyderabad", "Telangana"),
        ("Kolkata", "West Bengal")
    ]

    start_date = date(1950, 1, 1)
    end_date = date(2020, 12, 31)

    for i in range(1, 10001):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)

        city, state = random.choice(locations)

        dob_days = random.randint(
            0,
            (end_date - start_date).days
        )

        date_of_birth = start_date + timedelta(days=dob_days)

        registration_date = date(
            random.randint(2020, 2026),
            random.randint(1, 12),
            random.randint(1, 28)
        )

        patient = {
            "patient_id": f"P{i:05d}",
            "patient_name": f"{first_name} {last_name}",
            "gender": random.choice(genders),
            "date_of_birth": date_of_birth.isoformat(),
            "phone": f"9{random.randint(100000000, 999999999)}",
            "email": f"{first_name.lower()}.{last_name.lower()}{i}@example.com",
            "city": city,
            "state": state,
            "registration_date": registration_date.isoformat()
        }

        patients.append(patient)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "patient_id",
                "patient_name",
                "gender",
                "date_of_birth",
                "phone",
                "email",
                "city",
                "state",
                "registration_date"
            ]
        )

        writer.writeheader()
        writer.writerows(patients)

    print(f"Generated {len(patients)} patient records.")
    print(f"Output file: {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_patients()