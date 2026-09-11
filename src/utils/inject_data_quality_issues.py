import pandas as pd
import random
from pathlib import Path


SOURCE_FILE = Path("data/source/patients.csv")
OUTPUT_FILE = Path("data/simulated/patients.csv")


def inject_patient_issues():
    # Read clean source data
    df = pd.read_csv(
        SOURCE_FILE,
        dtype={"phone": "string"}
    )

    # --------------------------------------------------
    # Issue 1: Add duplicate patient records
    # --------------------------------------------------

    duplicate_records = df.sample(
        n=50,
        random_state=42
    )

    df = pd.concat(
        [df, duplicate_records],
        ignore_index=True
    )

    # --------------------------------------------------
    # Issue 2: Add missing email values
    # --------------------------------------------------

    missing_email_indexes = df.sample(
        n=50,
        random_state=100
    ).index

    df.loc[
        missing_email_indexes,
        "email"
    ] = None

    # --------------------------------------------------
    # Issue 3: Add invalid phone numbers
    # --------------------------------------------------

    invalid_phone_indexes = df.sample(
        n=20,
        random_state=200
    ).index

    invalid_phones = [
        "12345",
        "9876",
        "ABC1234567",
        "99999",
        "123456789"
    ]

    for index in invalid_phone_indexes:
        df.loc[
            index,
            "phone"
        ] = random.choice(invalid_phones)

    # Create output directory
    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    # Write simulated data
    df.to_csv(
        OUTPUT_FILE,
        index=False
    )

    print(f"Original records: {len(df) - 50}")
    print(f"Final records: {len(df)}")
    print("Duplicate records added: 50")
    print("Missing email values added: 50")
    print("Invalid phone numbers added: 20")
    print(f"Output file: {OUTPUT_FILE}")

def inject_provider_issues():
    df = pd.read_csv(
        "data/source/providers.csv",
        dtype={"hospital_id": "string"}
    )

    # Add 10 invalid hospital IDs
    invalid_indexes = df.sample(
        n=10,
        random_state=300
    ).index

    for index in invalid_indexes:
        df.loc[index, "hospital_id"] = "H9999"

    # Add 20 missing specializations
    missing_indexes = df.sample(
        n=20,
        random_state=400
    ).index

    df.loc[
        missing_indexes,
        "specialization"
    ] = None

    output_file = Path(
        "data/simulated/providers.csv"
    )

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    df.to_csv(
        output_file,
        index=False
    )

    print("Provider records:", len(df))
    print("Invalid hospital IDs added: 10")
    print("Missing specialization values added: 20")
    print(f"Output file: {output_file}")

def inject_appointment_issues():
    # Read clean appointment data
    df = pd.read_csv(
        "data/source/appointments.csv"
    )

    # --------------------------------------------------
    # Issue 1: Add invalid patient IDs
    # --------------------------------------------------

    invalid_patient_indexes = df.sample(
        n=20,
        random_state=500
    ).index

    for index in invalid_patient_indexes:
        df.loc[index, "patient_id"] = "P99999"

    # --------------------------------------------------
    # Issue 2: Add invalid provider IDs
    # --------------------------------------------------

    invalid_provider_indexes = df.sample(
        n=20,
        random_state=600
    ).index

    for index in invalid_provider_indexes:
        df.loc[index, "provider_id"] = "PR9999"

    # --------------------------------------------------
    # Issue 3: Add duplicate appointment records
    # --------------------------------------------------

    duplicate_records = df.sample(
        n=50,
        random_state=700
    )

    df = pd.concat(
        [df, duplicate_records],
        ignore_index=True
    )

    # --------------------------------------------------
    # Write simulated data
    # --------------------------------------------------

    output_file = Path(
        "data/simulated/appointments.csv"
    )

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    df.to_csv(
        output_file,
        index=False
    )

    print(f"Original records: {len(df) - 50}")
    print(f"Final records: {len(df)}")
    print("Invalid patient IDs added: 20")
    print("Invalid provider IDs added: 20")
    print("Duplicate appointment records added: 50")
    print(f"Output file: {output_file}")

def inject_claim_issues():
    # Read clean medical claims data
    df = pd.read_csv(
        "data/source/medical_claims.csv"
    )

    # --------------------------------------------------
    # Issue 1: Add negative claim amounts
    # --------------------------------------------------

    negative_amount_indexes = df.sample(
        n=20,
        random_state=800
    ).index

    for index in negative_amount_indexes:
        df.loc[index, "claim_amount"] = -abs(
            df.loc[index, "claim_amount"]
        )

    # --------------------------------------------------
    # Issue 2: Add invalid patient IDs
    # --------------------------------------------------

    invalid_patient_indexes = df.sample(
        n=20,
        random_state=900
    ).index

    for index in invalid_patient_indexes:
        df.loc[index, "patient_id"] = "P99999"

    # --------------------------------------------------
    # Issue 3: Add missing claim status
    # --------------------------------------------------

    missing_status_indexes = df.sample(
        n=20,
        random_state=1000
    ).index

    df.loc[
        missing_status_indexes,
        "claim_status"
    ] = None

    # --------------------------------------------------
    # Issue 4: Add duplicate claim records
    # --------------------------------------------------

    duplicate_records = df.sample(
        n=50,
        random_state=1100
    )

    df = pd.concat(
        [df, duplicate_records],
        ignore_index=True
    )

    # --------------------------------------------------
    # Write simulated data
    # --------------------------------------------------

    output_file = Path(
        "data/simulated/medical_claims.csv"
    )

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    df.to_csv(
        output_file,
        index=False
    )

    print(f"Original records: {len(df) - 50}")
    print(f"Final records: {len(df)}")
    print("Negative claim amounts added: 20")
    print("Invalid patient IDs added: 20")
    print("Missing claim status values added: 20")
    print("Duplicate claim records added: 50")
    print(f"Output file: {output_file}")

if __name__ == "__main__":
    inject_patient_issues()
    inject_provider_issues()
    inject_appointment_issues()
    inject_claim_issues()