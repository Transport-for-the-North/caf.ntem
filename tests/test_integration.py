# Built-Ins
import argparse
import pathlib

# Local Imports
# import pytest
import caf.ntem as ntem


def scenario() -> ntem.Scenarios:
    return ntem.Scenarios.CORE


# @pytest.fixture(name = "access_dir", scope="module")
def access_dir() -> pathlib.Path:
    """Fixture to create a temporary directory for testing."""
    return pathlib.Path(r"G:\raw_data\NTEM_SQLITE\test_data_set")


# @pytest.fixture(name = "output_dir", scope="module")
def output_dir() -> pathlib.Path:
    """Fixture to create a temporary directory for testing."""
    return pathlib.Path(r"F:/NTEM_SQLITE_DB/test")


def build_database(
    access_dir: pathlib.Path,
    output_dir: pathlib.Path,
    scenarios: list[ntem.Scenarios],
) -> None:
    """Build the NTEM database."""
    # Create the NTEM database
    ntem.build.build_db(
        access_dir=access_dir,
        output_dir=output_dir,
        scenarios=scenarios,
    )


def test_planning_query(db_handler: ntem.DataBaseHandler) -> None:

    ntem.PlanningQuery(
        2015,
        2021,
        2023,
        scenario=scenario(),
        output_zoning=ntem.ZoningSystems.AUTHORITY,
        filter_zoning_system=ntem.ZoningSystems.AUTHORITY,
        filter_zone_names=["Newcastle upon Tyne"],
    ).query(db_handler).to_csv((output_dir() / "planning_query").with_suffix(".csv"))


def test_car_ownership_query(db_handler: ntem.DataBaseHandler) -> None:

    ntem.CarOwnershipQuery(
        2015,
        2021,
        2023,
        scenario=scenario(),
        output_zoning=ntem.ZoningSystems.AUTHORITY,
        filter_zoning_system=ntem.ZoningSystems.AUTHORITY,
        filter_zone_names=["Newcastle upon Tyne"],
    ).query(db_handler).to_csv((output_dir() / "carownership_query").with_suffix(".csv"))


def get_db_handler(db_path: pathlib.Path) -> ntem.DataBaseHandler:
    """Get the database handler."""
    db_handler = ntem.DataBaseHandler(host=db_path)
    return db_handler


def test_query(db_handler: ntem.DataBaseHandler) -> None:
    test_car_ownership_query(db_handler)
    test_planning_query(db_handler)


if __name__ == "__main__":
    # Run the test function
    parser = argparse.ArgumentParser(
        prog="NTEM Integration Test",
        description="Performs integration tests on the NTEM package",
    )
    parser.add_argument(
        "-b", "--build", default="y", help="Whether to test the build process (y/n)"
    )
    args = parser.parse_args()
    build = True
    if args.build.lower() == "y" or args.build.lower() == "yes":
        pass
    elif args.build.lower() == "n" or args.build.lower() == "no":
        build = False
    else:
        raise ValueError("Invalid value for --build. Use 'y' or 'n'.")

    if build:
        # Build the database
        build_database(
            access_dir=access_dir(),
            output_dir=output_dir(),
            scenarios=[scenario()],
        )

    test_query(get_db_handler(output_dir() / "NTEM.sqlite"))
