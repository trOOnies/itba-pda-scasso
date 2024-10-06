import os

from tasks.etl import extract_data, load_to_redshift, transform_data


def main():
    local_fd = "/home/troonies/repos/itba/itba-pda-scasso/local"

    extracted_path = extract_data(
        extracted_fd_path=os.path.join(local_fd, "extracted"),
    )
    # extracted_path = "/home/troonies/repos/itba/itba-pda-scasso/local/extracted/extracted_data_2024-09-21.pq"

    transformed_path = transform_data(
        extracted_path=extracted_path,
        transformed_fd_path=os.path.join(local_fd, "transformed"),
    )

    load_to_redshift(
        transformed_path=transformed_path,
        redshift_table="test",
    )


if __name__ == "__main__":
    main()
