import pandas as pd
import dask.dataframe as dd
import re
import datetime
from typing import List, Callable

student_id = 'mellon_id'
requests_file = 'csvFiles/toy_requests.csv'
discussions_file = 'csvFiles/toy_disc.csv'
assignments_file = 'csvFiles/toy_asgmt.csv' 
files = [requests_file,discussions_file,assignments_file]

class MeasureCalculator:
    def __init__(self):
        self.dfs = []

    #loads appropriate files into list dfs; reads files into a dask dataframe
    def read_files(self, filenames: List[str]) -> None: 
        for filename in filenames:
            df = dd.read_csv(filename, dtype={'submitted_at' : object, 'URL': object,'deleted_at':object,'topic_delayed_post_at':object, 'topic_type': object})
            self.dfs.append(df)

    #used within class only, determines the size of applicable table & function
    def __count(self, table:int, f: Callable) -> dd:
        df = self.dfs[table]
        return df.loc[f].groupby(student_id).size()

    #counts assignment between a given date range; assignments file will be at index 2 in list
    def assignment_interval_count(self, start: str, end: str) -> dd:
        submitted_column = 'submitted_at'
        return self.__count(2, lambda df: (df[submitted_column] >= start) & (df[submitted_column] < end))

    #counts late assignments between a given date range; assignments file will be at index 2 in list
    def late_assignment_count(self) -> dd:
        submitted_column = 'submitted_at'
        due_column = 'due_at'
        return self.__count(2, lambda df: df[submitted_column] > df[due_column])
        

    #32, # replies to forums posts 
    def replies_posts(self) -> dd:
        df = self.dfs[1]
        df = df[[student_id,'parent_discussion_entry_id']]
    
        return df.dropna(subset=['parent_discussion_entry_id']).groupby(student_id).size()

    #36, avg word count per post (discussion forum)
    def avg_wordcount_per_post(self) -> int:
        df = self.dfs[1]
        #gets all of the messages/ids where the message is a post(excluding a reply)
        df = df[[student_id,'parent_discussion_entry_id','message']][df['parent_discussion_entry_id'].isnull()] 
        d = {} 
        ids = [] 
        avgword = []

        for t in df.drop_duplicates(subset=student_id).itertuples():
            d[t[1]] = [0,0]

        for t in df[[student_id,'message']].itertuples():
                d[t[1]][1] += len(t[2])
                d[t[1]][0]+=1

        for k,v in d.items():
            ids.append(k)
            avgword.append(d[k][1]/d[k][0])
          
        p = pd.DataFrame(list(zip(ids,avgword)),columns = [student_id,'avg_wordcount'])
        df = dd.from_pandas(p,npartitions=1)
        return df

    #37, avg word ount per reply (discussion forum)
    def avg_wordcount_per_reply(self) -> dd:
        df = self.dfs[1]
        #gets all of the messages/ids where the message is a post(excluding a reply)
        df = df[[student_id,'parent_discussion_entry_id','message']].dropna(subset=['parent_discussion_entry_id'])
        d = {} 
        ids = [] 
        avgword = []

        for t in df.drop_duplicates(subset=student_id).itertuples():
            d[t[1]] = [0,0]

        for t in df[[student_id,'message']].itertuples():
                d[t[1]][1] += len(t[2])
                d[t[1]][0]+=1

        for k,v in d.items():
            ids.append(k)
            avgword.append(d[k][1]/d[k][0])
          
        p = pd.DataFrame(list(zip(ids,avgword)),columns = [student_id,'avg_wordcount'])
        df = dd.from_pandas(p,npartitions=1)
        return df

    # total number of submissions per student
    def assignment_submission_count(self) -> dd:
        df = self.dfs[2]
        return df.groupby(student_id).size()

    # avg student assignment score
    def assignment_average(self) -> dd:
        df = self.dfs[2]
        return df.groupby(student_id)['score'].mean()

    """

    The requests table is unrelated to the others, therefore, the following functions are proof of concept for now.
    Mostly for Toy Data Request table

    def visits_after_deadline(self) -> int:
        req = self.dfs[0] # get requests table
        asgmt = self.dfs[1] # get assignments table
        deadlines = req[[student_id, 'due_at', 'url']]
        views = asgmt[[student_id, 'timestamp', 'url']]
    
        return count of views after deadline with matching urls for each student
    
    
    def first_assignment_access(self) -> dd:
        req = self.dfs[0] # get requests table
        asgmt = self.dfs[1] # get assignments table
        first_access = req.loc[req.groupby([student_id, 'url'])['url'].idxmin()] # get student ids and the earliest urls for each assignment
        
        return assignments table grouped by student id, assignment info and the time between first assignment access and due date


    #45, visits to the gradebook (toyrequest sheet)
    def visits_gradebook(self) -> dd:
        df = self.dfs[0]
        df = df[[student_id,'web_application_controller']][df['web_application_controller'] == 'gradebooks']
        
        return requests table grouped by student id and use the size function (this is to get the # of times EACH student clicked gradebooks)


    #46, clicks on instructional content (toyrequest sheet)
    def clicks_instrucContent(self) -> dd:
        df = self.dfs[0]
        df = df[[student_id,'web_application_controller']][(df['web_application_controller'] == 'files') | (df['web_application_controller'] == 'context_modules') ]

        return requests table grouped by student id and use the size function (get the # of times EACH student clicked anything related to instructional content) 


   #48, files clicked/viewed (toyrequest sheet)
    def file_count(self) -> dd:
        df = self.dfs[0]
        df = df[[student_id,'web_application_controller']][df['web_application_controller'] == 'files']

        return requests table grouped by student id and use the size function (get the # of times EACH student clicked files) 


    #61, # of clicks total (toyrequest sheet) 
    def clicks(self) -> dd:
        df = self.dfs[0]

        return requests table grouped by student id and use the size function (get the # of times EACH student clicked ANYTHING) 

    #62, time spent online -> dd:
        req = self.dfs[0]
        disc = self.dfs[1]
        asmgt = self.dfs[2]
        
        
        return new dd with student Id and corresponding time spent online 
    """


if __name__ == "__main__":
    mc = MeasureCalculator()
    mc.read_files(files)

    df = mc.avg_wordcount_per_reply()
    print(df)

    df.to_csv(['csvFiles/random_testing.csv'])