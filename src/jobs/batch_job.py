from src.download_from_adls import download_file_from_adls
from src.transform import run_transformation


def run() -> None:
    print("Downloading raw file from ADLS...")
    local_file_path = download_file_from_adls()

    print("Starting Spark transformation...")
    run_transformation(local_file_path)

    print("Batch pipeline finished successfully.")


if __name__ == "__main__":
    run()