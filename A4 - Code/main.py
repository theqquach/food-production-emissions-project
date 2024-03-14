import pandas as pd
import os

DATA_DIR = "data"
CREATE_TABLE_TEMPLATE = """-- SQL DDL file: create_and_populate.sql

-- CREATE TABLES HERE

CREATE TABLE Country (
  id VARCHAR(3) PRIMARY KEY,
  name VARCHAR(255)
);

CREATE TABLE CropProduction (
  food_id VARCHAR(255),
  country_id VARCHAR(3),
  year YEAR,
  amount_produced_in_tonnes_per_hectare DECIMAL(10,2),
  PRIMARY KEY (food_id, country_id, year),
  FOREIGN KEY (country_id) REFERENCES Country(id)
);

CREATE TABLE Emissions (
  country_id VARCHAR(3),
  sector_id INT,
  substance VARCHAR(255),
  year YEAR,
  emission_amount DECIMAL(18,10),
  PRIMARY KEY (country_id, sector_id, substance, year),
  FOREIGN KEY (country_id) REFERENCES Country(id),
  FOREIGN KEY (sector_id) REFERENCES Sector(sector_id)
);

CREATE TABLE FoodConsumption (
  country_id VARCHAR(3),
  year YEAR,
  amount_consumed_in_tonnes DECIMAL(18,8),
  food_id VARCHAR(255),
  PRIMARY KEY (country_id, food_id, year),
  FOREIGN KEY (country_id) REFERENCES Country(id)
);

CREATE TABLE MeatProduction (
  country_id VARCHAR(3),
  year YEAR,
  food_id VARCHAR(255),
  amount_produced_in_tonnes DECIMAL(10,2),
  num_animals_slain INT,
  PRIMARY KEY (country_id, food_id, year),
  FOREIGN KEY (country_id) REFERENCES Country(id)
);

CREATE TABLE Sector (
  sector_id INT PRIMARY KEY,
  name VARCHAR(255)
);

"""


def preprocess_datasets(
    emissions_path, slain_path, meat_path, crop_production_path, crop_consumption_path
):
    # Note: This function was used to generate the csv files
    #       that were submitted on Canvas. Using the datasets
    #       from assignment 3, we applied multiple preprocessing
    #       steps outlined below to generate csv files that fit
    #       the schema of the RDB.
    #
    #       The processing code is included for transparency on
    #       how we obtained the submitted csv files.

    # EmissionRecord
    emissions_raw = pd.read_csv(emissions_path)

    year_columns = [col for col in emissions_raw.columns if col.isnumeric()]
    emissions = pd.melt(
        emissions_raw,
        id_vars=["Sector", "Substance", "EDGAR Country Code"],
        var_name="year",
        value_vars=year_columns,
        value_name="emission_amount",
    )

    # Rename all columns to lowercase
    emissions = emissions.rename(lambda x: x.lower(), axis=1)
    emissions = emissions.rename({"edgar country code": "country_id"}, axis=1)

    sector_to_id_map = {
        sector: id for id, sector in enumerate(emissions["sector"].unique())
    }

    emissions["sector_id"] = emissions["sector"].map(sector_to_id_map)

    emission_record = emissions[
        ["country_id", "sector_id", "substance", "year", "emission_amount"]
    ]

    # Sector
    sector = emissions[["sector_id", "sector"]].drop_duplicates().reset_index(drop=True)
    sector = sector.rename({"sector_id": "id", "sector": "name"}, axis=1)

    # Country
    country = (
        emissions_raw[["EDGAR Country Code", "Country"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )
    country = country.rename({"EDGAR Country Code": "id", "Country": "name"}, axis=1)

    # MeatProduction
    slain = pd.read_csv(slain_path)
    meat_raw = pd.read_csv(meat_path)

    meat_columns = [col for col in meat_raw.columns if col.endswith("tonnes)")]
    meat = pd.melt(
        meat_raw,
        id_vars=["Entity", "Code", "Year"],
        var_name="meat_type",
        value_vars=meat_columns,
        value_name="tonnes",
    )

    def get_animals_slain(row):
        slain_row = slain.query(f"(Code == '{row['Code']}') & (Year == {row['Year']})")
        if row["meat_type"] == "Sheep and Goat ":
            return slain_row[
                ["Goats (goats slaughtered)", "Sheep (sheeps slaughtered)"]
            ].sum()[0]
        elif row["meat_type"] == "Beef and Buffalo ":
            return slain_row[["Cattle (cattle slaughtered)"]].sum()[0]
        elif row["meat_type"] == "Pigmeat ":
            return slain_row[["Pigs (pigs slaughtered)"]].sum()[0]
        elif row["meat_type"] == "Poultry ":
            return slain_row[
                ["Chicken (chicken slaughtered)", "Turkey (turkeys slaughtered)"]
            ].sum()[0]
        else:
            return 0

    meat["meat_type"] = meat["meat_type"].replace(r"\(tonnes\)", "", regex=True)
    meat["num_animals_slain"] = meat.apply(lambda row: get_animals_slain(row), axis=1)

    # Rename columns
    meat = meat.rename(
        {
            "Code": "country_id",
            "Year": "year",
            "tonnes": "amount_produced_in_tonnes",
            "meat_type": "food_id",
        },
        axis=1,
    )
    meat_production = meat[
        [
            "food_id",
            "country_id",
            "year",
            "amount_produced_in_tonnes",
            "num_animals_slain",
        ]
    ]

    # CropProduction
    crop_production_path = "../../../Downloads/crop_production.csv"
    crop_production_raw = pd.read_csv(crop_production_path)
    crop_production_raw = crop_production_raw.rename(
        columns={
            "LOCATION": "country_id",
            "TIME": "year",
            "Value": "amount_produced_in_tonnes_per_hectare",
            "Commodity": "food_id",
        }
    )
    crop_production_raw["food_id"] = crop_production_raw["food_id"].str.title()
    crop_production = crop_production_raw[
        ["food_id", "country_id", "year", "amount_produced_in_tonnes_per_hectare"]
    ]

    crop_consumption_raw = pd.read_csv(crop_consumption_path)
    crop_consumption_raw = crop_consumption_raw.rename(
        columns={
            "LOCATION": "country_id",
            "TIME": "year",
            "Value": "amount_consumed_in_tonnes",
            "Commodity": "food_id",
        }
    )
    crop_consumption_raw["food_id"] = crop_consumption_raw["food_id"].str.title()
    crop_consumption = crop_consumption_raw[
        ["food_id", "country_id", "year", "amount_consumed_in_tonnes"]
    ]

    # Consumption
    meat_consumption_path = "../../../Downloads/meat_consumption_worldwide.csv"
    meat_consumption_raw = pd.read_csv(meat_consumption_path)
    meat_consumption_raw = meat_consumption_raw.rename(
        {"LOCATION": "Code", "TIME": "Year"}, axis=1
    )

    # Dictionary to map 'SUBJECT' to 'meat_type'
    subject_to_meat_type = {
        "BEEF": "Beef and Buffalo ",
        "PIG": "Pigmeat ",
        "POULTRY": "Poultry ",
        "SHEEP": "Sheep and Goat ",
    }

    # Apply the mapping to the DataFrame
    meat_consumption_raw["food_id"] = meat_consumption_raw["SUBJECT"].map(
        subject_to_meat_type
    )
    meat_consumption_raw = meat_consumption_raw.drop(columns=["SUBJECT", "MEASURE"])
    meat_consumption = meat_consumption_raw.rename(
        {"Code": "country_id", "Value": "amount_consumed_in_tonnes", "Year": "year"},
        axis=1,
    )

    # Only keep countries in country table
    food_consumption = pd.concat([meat_consumption, crop_consumption])
    # Remove projected values (above 2024)
    food_consumption = food_consumption[
        (food_consumption.country_id.isin(country.id)) & (food_consumption.year < 2024)
    ].reset_index(drop=True)

    # Final tables in RDB schema
    emissions.to_csv("emissions.csv")
    sector.to_csv("sector.csv")
    country.to_csv("country.csv")
    meat_production.to_csv("meat_production.csv")
    crop_production.to_csv("crop_production.csv")
    food_consumption.to_csv("food_consumption.csv")


def insert_datasets_to_database():
    """Dynamically generate INSERT statements from the datasets."""

    # Tuple containing (table_name, filepath)
    datasets = [
        (f.replace(".csv", ""), os.path.join(DATA_DIR, f))
        for f in os.listdir(DATA_DIR)
        if f.endswith(".csv")
    ]

    with open("create_and_populate.sql", "w") as f:

        print("Inserting the 'CREATE TABLE' statements...")
        f.write(CREATE_TABLE_TEMPLATE)

        print("Iterating over the datasets...")
        for table_name, filepath in datasets:
            df = pd.read_csv(filepath)
            columns = [col.lower() for col in df.columns]

            for row in df.iterrows():
                values = [
                    f"'{value}'" if isinstance(value, str) else str(value)
                    for value in row[1]
                ]
                f.write(
                    f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});\n"
                )

            print(f"Done with {table_name}.")
            f.write("\n\n")


if __name__ == "__main__":
    insert_datasets_to_database()
