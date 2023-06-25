# DE_interview_assignment

The DE_interview_assignment program is a data preprocessing application that takes in two datasets and a list of countries as inputs. It performs the following operations on the datasets:

- Joins the two datasets on common IDs.
- Drops columns containing personal identifiable information.
- Filters rows based on the given list of countries.
- Renames the columns for the easier readability to the business users. 


### Program consists of two files:

1. data_preprocessing.py: This file contains the main logic for data preprocessing. It defines functions for joining datasets, dropping columns, and filtering rows based on countries.
2. tests.py: This file contains unit tests to verify the correctness of the data preprocessing functions. It uses the unittest framework and the chispa library for DataFrame assertions.
