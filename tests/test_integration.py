# Built-Ins
import argparse
import pathlib

import pandas as pd

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


def control_planning_result() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "zone": pd.Series(["E08000021", "E08000021"]),
            "year": pd.Series([2018, 2023]),
            "16 to 74": pd.Series([213615, 218263], dtype="float64"),
            "75 +": pd.Series([18681, 19944], dtype="float64"),
            "Households": pd.Series([121909, 125925], dtype="float64"),
            "Jobs": pd.Series([183804, 188128], dtype="float64"),
            "Less than 16": pd.Series([50293, 51719], dtype="float64"),
            "Workers": pd.Series([122827, 128063], dtype="float64"),
        },
    ).set_index(["zone", "year"])


def test_planning_query(db_handler: ntem.DataBaseHandler) -> None:

    planning = ntem.PlanningQuery(
        2018,
        2023,
        scenario=scenario(),
        output_zoning=ntem.ZoningSystems.AUTHORITY,
        filter_zoning_system=ntem.ZoningSystems.AUTHORITY,
        filter_zone_names=["Newcastle upon Tyne"],
    ).query(db_handler)
    planning.to_csv((output_dir() / "planning_query").with_suffix(".csv"))
    # We round as we are comparing to TEMPro which gives results rounded to the nearest integer.
    pd.testing.assert_frame_equal(
        planning.round(0),
        right=control_planning_result(),
        check_names=False,
    )


def control_tebd_result() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "zone": pd.Series(["E08000021", "E08000021"]),
            "time_period": pd.Series(
                [
                    "Weekday AM peak period (0700 - 0959)",
                    "Weekday AM peak period (0700 - 0959)",
                ]
            ),
            "year": pd.Series([2018, 2023]),
            "attraction_trip_end": pd.Series([33749, 34989], dtype="float64"),
            "production_trip_end": pd.Series([24102, 25679], dtype="float64"),
        },
    ).set_index(["zone", "time_period", "year"])


def test_trip_end_by_direction_query(db_handler: ntem.DataBaseHandler) -> None:

    test_tebd = ntem.TripEndByDirectionQuery(
        2018,
        2023,
        scenario=scenario(),
        trip_type=ntem.TripType.PA,
        mode_filter=[ntem.Mode.CAR_DRIVER],
        aggregate_mode=True,
        purpose_filter=[ntem.Purpose.HB_WORK],
        aggregate_purpose=True,
        output_zoning=ntem.ZoningSystems.AUTHORITY,
        time_period_filter=[ntem.TimePeriod.AM],
        filter_zoning_system=ntem.ZoningSystems.AUTHORITY,
        filter_zone_names=["Newcastle upon Tyne"],
    ).query(db_handler)
    test_tebd.to_csv((output_dir() / "tripend_by_direction_query").with_suffix(".csv"))
    # We round as we are comparing to TEMPro which gives results rounded to the nearest integer.
    pd.testing.assert_frame_equal(
        test_tebd.round(0),
        right=control_tebd_result(),
        check_names=False,
    )


def control_tebca_result() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "zone": pd.Series(
                [
                    "E08000021",
                    "E08000021",
                    "E08000021",
                    "E08000021",
                    "E08000021",
                    "E08000021",
                    "E08000021",
                    "E08000021",
                ]
            ),
            "car_availability_type": pd.Series(
                [
                    "Households with no cars",
                    "Households with 1 adult and 1 car",
                    "Households with 2+ adults and 1 car",
                    "Households with 2+ adults and 2+ cars",
                    "Households with no cars",
                    "Households with 1 adult and 1 car",
                    "Households with 2+ adults and 1 car",
                    "Households with 2+ adults and 2+ cars",
                ]
            ),
            "year": pd.Series(
                [
                    2018,
                    2018,
                    2018,
                    2018,
                    2023,
                    2023,
                    2023,
                    2023,
                ]
            ),
            "value": pd.Series(
                [7962, 19535, 79897, 100589, 7720, 21787, 82526, 108576], dtype="float64"
            ),
        },
    ).set_index(["zone", "car_availability_type", "year"])


def test_trip_end_by_car_av_query(db_handler: ntem.DataBaseHandler) -> None:

    test_tebca = ntem.TripEndByCarAvailabilityQuery(
        2018,
        2023,
        scenario=scenario(),
        mode_filter=[ntem.Mode.CAR_DRIVER],
        aggregate_mode=True,
        purpose_filter=[ntem.Purpose.HB_WORK],
        aggregate_purpose=True,
        output_zoning=ntem.ZoningSystems.AUTHORITY,
        filter_zoning_system=ntem.ZoningSystems.AUTHORITY,
        filter_zone_names=["Newcastle upon Tyne"],
    ).query(db_handler)
    test_tebca.to_csv((output_dir() / "tripend_by_car_av_query").with_suffix(".csv"))
    # We round as we are comparing to TEMPro which gives results rounded to the nearest integer
    pd.testing.assert_frame_equal(
        test_tebca.round(0),
        right=control_tebca_result(),
        check_names=False,
    )


def control_car_ownership_result() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "zone": pd.Series(["E08000021", "E08000021"]),
            "year": pd.Series([2018, 2023]),
            "1 Car": pd.Series([48480, 50433], dtype="float64"),
            "2 Cars": pd.Series([19285, 19913], dtype="float64"),
            "3+ Cars": pd.Series([5038, 5248], dtype="float64"),
            "No Car": pd.Series([49105, 50331], dtype="float64"),
        },
    ).set_index(["zone", "year"])


def test_car_ownership_query(db_handler: ntem.DataBaseHandler) -> None:

    car_ownership_test = ntem.CarOwnershipQuery(
        2018,
        2023,
        scenario=scenario(),
        output_zoning=ntem.ZoningSystems.AUTHORITY,
        filter_zoning_system=ntem.ZoningSystems.AUTHORITY,
        filter_zone_names=["Newcastle upon Tyne"],
    ).query(db_handler)
    car_ownership_test.to_csv((output_dir() / "carownership_query").with_suffix(".csv"))
    # We round as we are comparing to TEMPro which gives results rounded to the nearest integer
    pd.testing.assert_frame_equal(
        car_ownership_test.round(0),
        right=control_car_ownership_result(),
        check_names=False,
    )


def get_db_handler(db_path: pathlib.Path) -> ntem.DataBaseHandler:
    """Get the database handler."""
    db_handler = ntem.DataBaseHandler(host=db_path)
    return db_handler


def test_query(db_handler: ntem.DataBaseHandler) -> None:
    test_trip_end_by_car_av_query(db_handler)
    #test_car_ownership_query(db_handler)
    test_planning_query(db_handler)
    test_trip_end_by_direction_query(db_handler)


if __name__ == "__main__":
    # Run the test function
    parser = argparse.ArgumentParser(
        prog="NTEM Integration Test",
        description="Performs integration tests on the NTEM package",
    )
    parser.add_argument(
        "-b", "--build", default="y", help="Whether to test the build process (y/n)"
    )
    parser.add_argument(
        "-adb",
        "--access_database",
        default=None,
        help="Acess database path used for building the NTEM database (only nessesary if --build is set to y)",
    )
    parser.add_argument(
        "-o",
        "--output_path",
        default=None,
        help="Where to output the test outputs.",
    )
    parser.add_argument(
        "-db",
        "--database",
        default=None,
        help="Database to use for the tests (if not building the database)",
    )

    args = parser.parse_args()
    build = True
    if args.build.lower() == "y" or args.build.lower() == "yes":
        pass
    elif args.build.lower() == "n" or args.build.lower() == "no":
        build = False
    else:
        raise ValueError("Invalid value for --build. Use 'y' or 'n'.")

    if args.access_database is not None:
        access_dir_ = pathlib.Path(args.access_database)
    else:
        access_dir_ = access_dir()
    if args.output_path is not None:
        output_dir_ = pathlib.Path(args.output_path)
    else:
        output_dir_ = output_dir()
    if args.database is not None and not build:
        db_path = pathlib.Path(args.database)
    else:
        db_path = output_dir_ / "NTEM.sqlite"

    if build:
        # Build the database
        if (db_path).exists():
            raise FileExistsError(
                f"Database already exists at {db_path}. Delete it to test build functionality."
            )
        build_database(
            access_dir=access_dir_,
            output_dir=output_dir_,
            scenarios=[scenario()],
        )

    test_query(get_db_handler(db_path))
