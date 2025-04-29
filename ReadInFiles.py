# %%
import pandas as pd
import time
import numpy as np
import pyreadstat

# %%
def process_CAN_file (inputpath, filename):
    csv_path = inputpath + filename
    try:
        df = pd.read_spss(path + filename, 
        usecols = ["staterr", "chid", "afcarsid", "inciddt", "rptdt", "rpdispdt", "mal1lev", "mal2lev","mal3lev","mal4lev","maldeath"],
        convert_categoricals = False
        )

        df["Event_Type"] = "CAN Investigation" 

        #agency needs to be updated to FIPS code
        df["Agency_ID"] = df["staterr"]

        df.rename(columns={"afcarsid":"Child_ID"}, inplace=True)

        df["Start_Date"] = np.where(df["inciddt"].isna(),df["rptdt"],df["inciddt"])
        #SPSS date9 format is not read in correctly by pandas/pyreadstat, so manually adjusting.
        df["Start_Date"] = pd.to_datetime(((df['Start_Date']/86400)-141428), unit = 'D') 

        df["Start_Condition"] = ""

        df.rename(columns={"rpdispdt":"End_Date"}, inplace=True)
        #SPSS date9 format is not read in correctly by pandas/pyreadstat, so manually adjusting.
        df["End_Date"] = pd.to_datetime(((df['End_Date']/86400)-141428), unit = 'D')

        df["End_Condition"] = np.where(df["maldeath"] == 1, 
            0,
            df[["mal1lev", "mal2lev","mal3lev","mal4lev"]].min(axis="columns")
            )
        values = [0,1,2,3,4,5,6,7,8,88,99]
        labels = ['Death',
                'Substantiated',
                'Indicated or reason to suspect',
                'Alternative response victim', 
                'Alternative response nonvictim', 
                'Unsubstantiated', 
                'Unsubstantiated due to intentionally false reporting', 
                'Closed-no finding',
                'No alleged maltreatment',
                'Other',
                'Unknown or missing']
        df["End_Condition"] = df['End_Condition'].map(dict(zip(values, labels)))

        df["Notes"] = ""

        df["Source"] = filename
        return(df)
    except FileNotFoundError:
        print(f"Error: SAV file not found at '{csv_path}'")
        return pd.DataFrame()
    except pd.errors.EmptyDataError:
        print(f"Error: SAV file at '{csv_path}' is empty")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()

def process_Prevention_Plan_file (inputpath, filename):
    csv_path = inputpath + filename
    try:
        df = pd.read_spss(path + filename, 
        usecols = ["staterr", "chid", "afcarsid", "inciddt", "rptdt", "rpdispdt", "mal1lev", "mal2lev","mal3lev","mal4lev","maldeath"],
        convert_categoricals = False
        )

        df = df[(df['f1_title_iv_agency'] != 33) & (df['reporting_period_name'] != '2023A')]


        df["Event_Type"] = "Prevention Plan" 

        #agency needs to be updated to FIPS code
        df["Agency_ID"] = df["staterr"]

        df.rename(columns={"afcarsid":"Child_ID"}, inplace=True)

        df["Start_Date"] = np.where(df["inciddt"].isna(),df["rptdt"],df["inciddt"])
        #SPSS date9 format is not read in correctly by pandas/pyreadstat, so manually adjusting.
        df["Start_Date"] = pd.to_datetime(((df['Start_Date']/86400)-141428), unit = 'D') 

        df["Start_Condition"] = ""

        df.rename(columns={"rpdispdt":"End_Date"}, inplace=True)
        #SPSS date9 format is not read in correctly by pandas/pyreadstat, so manually adjusting.
        df["End_Date"] = pd.to_datetime(((df['End_Date']/86400)-141428), unit = 'D')

        df["End_Condition"] = np.where(df["maldeath"] == 1, 
            0,
            df[["mal1lev", "mal2lev","mal3lev","mal4lev"]].min(axis="columns")
            )
        values = [0,1,2,3,4,5,6,7,8,88,99]
        labels = ['Death',
                'Substantiated',
                'Indicated or reason to suspect',
                'Alternative response victim', 
                'Alternative response nonvictim', 
                'Unsubstantiated', 
                'Unsubstantiated due to intentionally false reporting', 
                'Closed-no finding',
                'No alleged maltreatment',
                'Other',
                'Unknown or missing']
        df["End_Condition"] = df['End_Condition'].map(dict(zip(values, labels)))

        df["Notes"] = ""

        df["Source"] = filename
        return(df)
    except FileNotFoundError:
        print(f"Error: SAV file not found at '{csv_path}'")
        return pd.DataFrame()
    except pd.errors.EmptyDataError:
        print(f"Error: SAV file at '{csv_path}' is empty")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()






# %%
if __name__ == '__main__':
    path = r'C:\\Users\\malcolm.hale\\Non_Sync\\NCANDS File\\'
    filename = r'allstate2022child_cm2023.sav'
    Test_df = process_CAN_file(inputpath=path,filename=filename)

# %%
