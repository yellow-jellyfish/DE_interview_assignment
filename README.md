# DE_interview_assignment

The DE_interview_assignment program is a data preprocessing application that takes in two datasets paths and a list of countries as inputs. It performs the following operations on the datasets:

- Joins the two datasets on common IDs.
- Drops columns containing personal identifiable information.
- Filters rows based on the given list of countries.
- Renames the columns for the easier readability to the business users.
- Returns the new dataset in the client_data file.  

### Program consists of following files:
1. data_preprocessing.py: This file contains the main logic for data preprocessing. It defines functions for joining datasets, dropping columns, filtering rows and renaming columns.
2. tests.py: This file contains unit tests to verify the correctness of the data preprocessing functions. It uses the unittest framework and the chispa library for DataFrame assertions.
3. dataset_one.csv and dataset_two.csv: Client datasets containing information about the clients and their financial details. Each dataset should have an "id" column that serves as the common identifier for joining.
4. requirements.txt: This file lists the dependencies required to run the DE_interview_assignment program. 
