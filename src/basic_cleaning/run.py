#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb

import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    df = pd.read_csv(artifact_local_path)
    logger.info(f"Read input artifact CSV file and loaded data into a DataFrame")

    # Drop outliers
    idx = df["price"].between(args.min_price, args.max_price)
    df = df[idx].copy()
    logger.info(f"Dropped outliers based on specified min and max price")

    # Convert last_review to datetime
    df["last_review"] = pd.to_datetime(df["last_review"])
    logger.info(f"Converted last_review column to datetime format")

    # Save cleaned dataset to local file
    df.to_csv("clean_sample.csv", index=False)
    logger.info(f"Saved cleaned dataset to clean_sample.csv file")

    # Upload local file to wandb
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)
    logger.info(
        f"Uploaded clean_sample.csv to wandb as artifact {args.output_artifact}"
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="the name of the input artifact",
        required=True,
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="the name of the output artifact",
        required=True,
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="the type of the output artifact (e.g. 'data')",
        required=True,
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="a short description of the output artifact",
        required=True,
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="filter out listings with price below this value",
        required=True,
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="filter out listings with price above this value",
        required=True,
    )

    args = parser.parse_args()

    go(args)
