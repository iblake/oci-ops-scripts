import json
import csv
import subprocess
import argparse

def run_oci_command(command):
    """Executes an OCI CLI command and returns the parsed JSON output."""
    try:
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error executing OCI CLI: {result.stderr}")
            return [] if "list" in command else {}
        return json.loads(result.stdout).get("data", [])
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        print(f"Unexpected error: {e}")
        return [] if "list" in command else {}

def get_pdb_list(profile, compartment_id):
    """Retrieves the list of PDBs."""
    print("Fetching PDBs...")
    return run_oci_command(["oci", "db", "pluggable-database", "list", "--limit", "1000", "--compartment-id", compartment_id, "--profile", profile])

def get_cdb_mapping(profile, compartment_id):
    """Retrieves a mapping of CDB IDs to names and DB Home IDs."""
    print("Fetching CDBs...")
    databases = run_oci_command(["oci", "db", "database", "list", "--limit", "2000", "--compartment-id", compartment_id, "--profile", profile])
    return {db.get("id", "Unknown ID"): {"CDB_Name": db.get("db-name", "Unknown CDB"), "DB_Home_ID": db.get("db-home-id", "Unknown DB Home")} for db in databases}

def get_db_home_mapping(profile, compartment_id):
    """Retrieves a mapping of DB Home IDs to their display names."""
    print("Fetching DB Homes...")
    db_homes = run_oci_command(["oci", "db", "db-home", "list", "--limit", "1000", "--compartment-id", compartment_id, "--profile", profile])
    return {home.get("id", "Unknown DB Home"): home.get("display-name", "Unknown Oracle Home") for home in db_homes}

def get_compartment_name(profile, compartment_id):
    """Retrieves the name of the compartment given its OCID."""
    print("Fetching Compartment Name...")
    compartment = run_oci_command(["oci", "iam", "compartment", "get", "--compartment-id", compartment_id, "--profile", profile])
    return compartment.get("name", "Unknown Compartment") if compartment else "Unknown Compartment"

def extract_pdb_cdb_mapping(pdbs, cdb_mapping, db_home_mapping, compartment_id, compartment_name):
    """Creates a structured mapping of PDBs to their CDBs, Oracle Homes, and Compartment Name."""
    return [
        {
            "PDB_Name": pdb.get("pdb-name", "Unknown PDB"),
            "CDB_Name": cdb_mapping.get(pdb.get("container-database-id", "Unknown CDB"), {}).get("CDB_Name", "Unknown CDB"),
            "Oracle_Home": db_home_mapping.get(cdb_mapping.get(pdb.get("container-database-id"), {}).get("DB_Home_ID"), "Unknown Oracle Home"),
            "Compartment_Name": compartment_name,
            "CDB_ID": pdb.get("container-database-id", "Unknown CDB"),
            "Compartment_ID": compartment_id
        }
        for pdb in pdbs
    ]

def save_to_csv(mapping, output_file):
    """Saves data to a CSV file."""
    try:
        with open(output_file, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["PDB_Name", "CDB_Name", "Oracle_Home", "Compartment_Name", "CDB_ID", "Compartment_ID"])
            writer.writeheader()
            writer.writerows(mapping)
        print(f"CSV generated: {output_file}")
    except Exception as e:
        print(f"Error saving CSV: {e}")

def main():
    """Runs the full extraction process."""
    parser = argparse.ArgumentParser(description="Extracts the relationship of PDBs and CDBs from OCI.")
    parser.add_argument("--profile", required=True, help="OCI profile to use.")
    parser.add_argument("--compartment-id", required=True, help="OCID of the compartment to query.")
    args = parser.parse_args()
    
    print("Starting extraction...")
    pdbs = get_pdb_list(args.profile, args.compartment_id)
    if not pdbs:
        print("No PDB data found. Check OCI CLI command and permissions.")
        return
    
    cdb_mapping = get_cdb_mapping(args.profile, args.compartment_id)
    db_home_mapping = get_db_home_mapping(args.profile, args.compartment_id)
    compartment_name = get_compartment_name(args.profile, args.compartment_id)
    mapping = extract_pdb_cdb_mapping(pdbs, cdb_mapping, db_home_mapping, args.compartment_id, compartment_name)
    save_to_csv(mapping, "pdb_cdb_mapping.csv")
    print("Process completed successfully.")

if __name__ == "__main__":
    main()
