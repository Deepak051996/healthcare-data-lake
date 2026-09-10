import csv
import random
from pathlib import Path


OUTPUT_FILE = Path("data/source/providers.csv")


def generate_providers():
    providers = []

    first_names = [
        "Amit", "Rahul", "Priya", "Neha", "Rohit",
        "Anita", "Vikas", "Pooja", "Arjun", "Sneha"
    ]

    last_names = [
        "Sharma", "Kumar", "Singh", "Patel", "Gupta",
        "Verma", "Mehta", "Joshi", "Yadav", "Mishra"
    ]

    specializations = [
        "Cardiology",
        "Neurology",
        "Orthopedics",
        "Pediatrics",
        "Dermatology",
        "Oncology",
        "General Medicine",
        "Gynecology",
        "Psychiatry",
        "ENT"
    ]

    cities = [
        ("Mumbai", "Maharashtra"),
        ("Pune", "Maharashtra"),
        ("Bengaluru", "Karnataka"),
        ("Delhi", "Delhi"),
        ("Chennai", "Tamil Nadu")
    ]

    for i in range(1, 1001):

        city, state = random.choice(cities)

        provider = {
            "provider_id": f"PR{i:04d}",
            "provider_name": f"{random.choice(first_names)} {random.choice(last_names)}",
            "specialization": random.choice(specializations),
            "hospital_id": f"H{random.randint(1, 100):04d}",
            "city": city,
            "state": state,
            "experience_years": random.randint(1, 35)
        }

        providers.append(provider)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as file:

        writer = csv.DictWriter(
            file,
            fieldnames=[
                "provider_id",
                "provider_name",
                "specialization",
                "hospital_id",
                "city",
                "state",
                "experience_years"
            ]
        )

        writer.writeheader()
        writer.writerows(providers)

    print(f"Generated {len(providers)} provider records.")
    print(f"Output file: {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_providers()