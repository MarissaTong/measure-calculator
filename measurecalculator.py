import pandas as pd
import dask.dataframe as dd
import re
import datetime
from typing import List, Callable

student_id = 'mellon_id'
requests_file = 'toy_requests.csv'
discussions_file = 'toy_disc.csv'
assignments_file = 'toy_asgmt.csv'

class MeasureCalculator:
    def __init__(self):
        self.dfs = []

    def read_files(self, filenames: List[str]) -> None:
        for filename in filenames:
            df = dd.read_csv(filename, dtype={'submitted_at' : object, 'URL': object})
            self.dfs.append(df)

    def __count(self, table:int, f: Callable) -> dd:
        df = self.dfs[table]
        return df.loc[f].groupby(student_id).size()

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
