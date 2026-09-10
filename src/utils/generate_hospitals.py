import csv
import random
from pathlib import Path


OUTPUT_FILE = Path("data/source/hospitals.csv")


def generate_hospitals():
   #def generate_hospitals():
    hospitals = []

    hospital_names = [
        "City Care Hospital",
        "Sunrise Medical Center",
        "Apollo Healthcare",
        "Global Health Hospital",
        "LifeCare Hospital"
    ]

    hospital_types = ["Private", "Government", "Trust"]

    cities = [
        ("Mumbai", "Maharashtra"),
        ("Pune", "Maharashtra"),
        ("Bengaluru", "Karnataka"),
        ("Delhi", "Delhi"),
        ("Chennai", "Tamil Nadu")
    ]

    for i in range(1, 101):
        city, state = random.choice(cities)

        hospital = {
            "hospital_id": f"H{i:04d}",
            "hospital_name": random.choice(hospital_names),
            "hospital_type": random.choice(hospital_types),
            "city": city,
            "state": state,
            "bed_count": random.randint(50, 1000)
        }

        hospitals.append(hospital)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "hospital_id",
                "hospital_name",
                "hospital_type",
                "city",
                "state",
                "bed_count"
            ]
        )

        writer.writeheader()
        writer.writerows(hospitals)

    print(f"Generated {len(hospitals)} hospital records.")
    print(f"Output file: {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_hospitals()