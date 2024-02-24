import json
import html
from pathlib import Path
import pandas as pd

src_metadata_dir = "./data/metadata/raw"
csv_out_dir = "./data/metadata/clean"

metadata_files = list(Path(src_metadata_dir).glob("*.json"))
print(metadata_files)


def clean_df(df):
    for col in df.columns:
        df[col] = df[col].astype("string")
        df[col] = df[col].apply(lambda x: html.unescape(x) if pd.notnull(x) else x)
        df[col] = df[col].str.strip(" \n\t\r")
        df[col] = df[col].str.replace("\n\t\r", " ")
        # replace multiple spaces with single space
        df[col] = df[col].str.replace("\\s+", " ", regex=True)
        # fill empty strings with NaN
        df[col] = df[col].replace("", pd.NA)
    return df


def process_judgment_links(df):
    df["temp_links"] = df["temp_link"].str.split("|")
    df["temp_link"] = df["temp_links"].str[0]
    expl_df = df.explode("temp_links")
    # if dairy no it "-0" and temp_link is same as temp_links, set all cells to NaN except the temp_link and diary no
    # they seem to be clubbing multiple judgments with diary no "-0", possibly like a catch all diary no if they don't have a diary no associated with some of the judgments. Judgment links for all such cases are coming in a single row.
    faulty_rows = (expl_df.diary_no == "-0") & (expl_df.temp_link != expl_df.temp_links)
    expl_df.loc[faulty_rows, expl_df.columns.difference(["temp_links", "diary_no"])] = (
        pd.NA
    )

    expl_df["temp_link"] = clean_df(expl_df[["temp_links"]])
    expl_df = expl_df.drop(columns=["temp_links"])
    # assert all rows contain temp_link with .pdf
    assert expl_df["temp_link"].str.contains(".pdf").all()

    # strip anything after the string ".pdf" in the temp_link column
    expl_df["temp_link"] = expl_df["temp_link"].str.extract(r"(.+?\.pdf)", expand=False)
    # extract language
    # prefix all temp_link that with judis with "jonew/"
    expl_df.reset_index(drop=True, inplace=True)
    mask = expl_df["temp_link"].str.startswith("judis")
    expl_df.loc[mask, "temp_link"] = "jonew/" + expl_df.loc[mask, "temp_link"]
    expl_df["language"] = expl_df["temp_link"].str.extract(
        r"_([A-Z]+).pdf", expand=False
    )
    # assert all rows that have language to contain "vernacular" also in the temp_link column and vice versa
    assert (
        expl_df["language"].notnull() == expl_df["temp_link"].str.contains("vernacular")
    ).all(), "vernacular should be part of the url if language is present"

    return expl_df


all_df = pd.DataFrame()

for mf in metadata_files:
    with open(mf, "r") as f:
        fjson = json.load(f)
        df = pd.DataFrame.from_dict(fjson["data"])
        all_df = pd.concat([all_df, df], ignore_index=True)
all_df = clean_df(all_df)
all_df = process_judgment_links(all_df)
Path(csv_out_dir).mkdir(parents=True, exist_ok=True)
all_df.to_csv(Path(csv_out_dir) / "judgments.csv", index=False)
