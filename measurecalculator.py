import pandas as pd
import dask.dataframe as dd
import re
import datetime
from typing import List, Callable

student_id = 'mellon_id'
requests_file = 'toy_requests.csv'
discussions_file = 'toy_disc.csv'
assignments_file = 'toy_asgmt.csv'
files = [requests_file,discussions_file,assignments_file]

class MeasureCalculator:
    def __init__(self):
        self.dfs = []

    #loads appropriate files into list dfs
    def read_files(self, filenames: List[str]) -> None: 
        for filename in filenames:
            df = dd.read_csv(filename, dtype={'submitted_at' : object, 'URL': object})
            self.dfs.append(df)

    #used within class only, determines the size of applicable table & function
    def __count(self, table:int, f: Callable) -> dd:
        df = self.dfs[table]
        return df.loc[f].groupby(student_id).size()

    #counts assignment between a given range; assignments file will be at index 2 in list
    def assignment_interval_count(self, start: str, end: str) -> dd:
        submitted_column = 'submitted_at'
        return self.__count(2, lambda df: (df[submitted_column] >= start) & (df[submitted_column] < end))

    def late_assignment_count(self) -> dd:
        submitted_column = 'submitted_at'
        due_column = 'due_at'
        return self.__count(2, lambda df: df[submitted_column] > df[due_column])
        

    """
    The requests table is unrelated to the others, therefore, the following functions are proof of concept for now.
    
    def visits_after_deadline(self) -> int:
        req = self.dfs[0] # get requests table
        asgmt = self.dfs[1] # get assignments table
        deadlines = req[[student_id, 'due_at', 'url']]
        views = asgmt[[student_id, 'timestamp', 'url']]
    
        return count of views after deadline with matching urls for each student
    
    def visits_after_deadline(self) -> int:
        req = self.dfs[0] # get requests table
        asgmt = self.dfs[1] # get assignments table
        deadlines = req[[student_id, 'due_at', 'url']]
        views = asgmt[[student_id, 'timestamp', 'url']]
    
        return count of views before deadline with matching urls for each student
    
    def first_assignment_access(self) -> dd:
        req = self.dfs[0] # get requests table
        asgmt = self.dfs[1] # get assignments table
        first_access = req.loc[req.groupby([student_id, 'url'])['url'].idxmin()] # get student ids and the earliest urls for each assignment
        
        return assignments table grouped by student id, assignment info and the time between first assignment access and due date

    """
    # avg word count per post (discussion forum)
    # def avg_wordcount_per_post(self) ->dd:


if __name__ == "__main__":
    mc = MeasureCalculator()
    mc.read_files(files)
    print(mc.dfs)

    mc.assignment_interval_count()
