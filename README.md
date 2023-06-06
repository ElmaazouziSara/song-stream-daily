# Daily Songs TOP 50 Model

A program that enables the user to daily select the TOP 50 songs per country and per user.
It could also run automatically every day.

Once you unzip the file, you will have a project folder structured as follows.

### Project's structure

    .
    ├── logs                   # Folder containg data of streaming logs.
    ├── output                 # Folder containing files of top 50 selection per day.
    ├── main.py                # Main script of the program.
    ├── requirements.txt
    └── README.md

# Install the requirements
Install dependencies with `pip install -r requirements.txt` inside your (virtual) environment...

Mainly requires: `pandas-2.0.2 & schedule`

> Notice: Tested with Python 3.10.11

After finishing dependencies installation, you need to make sure that your data is correctly placed into the right folder `logs`.
Each file should strictly follow the format `listen-YYYYMMDD.log`.

# 1. Run easily through commandline:

Change your directory to the root folder of the project:
`cd song_streams_daily_top_50`

## 1.1. Without daily automation:

Then start the program by running the python file:
`python3 main.py`

## 1.2. With the option of daily automation
To be able to activate the automatic daily execution of the program. You need to use the `--auto` flag.

`python3 main.py --auto`

## Maintainers

- Sara El Maazouzi - [Gmail](elmaazouzi.sara@gmail.com) -  [LinkedIn](https://www.linkedin.com/in/saraelm)
