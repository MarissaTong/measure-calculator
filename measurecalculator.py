import pandas as pd
import dask.dataframe as dd
import re
import datetime
from typing import List, Callable

student_id = 'mellon_id'
requests_file = 'csvFiles/toy_requests.csv'
discussions_file = 'csvFiles/toy_disc.csv'
assignments_file = 'csvFiles/toy_asgmt.csv' 
quizzes_file = 'csvFiles/toy_quiz.csv' 
files = [requests_file,discussions_file,assignments_file, quizzes_file]

class MeasureCalculator:
    def __init__(self):
        self.dfs = []

    #loads appropriate files into list dfs; reads files into a dask dataframe
    def read_files(self, filenames: List[str]) -> None: 
        for filename in filenames:
            df = dd.read_csv(filename, dtype={
                'submitted_at' : object, 
                'URL': object,
                'deleted_at': object,
                'topic_delayed_post_at': object, 
                'topic_type': object,
                'description' : object,
                'assignment_id': 'float64',
                'course_current_Score': 'float64',
                'course_final_score': 'float64',
                'extra_attempts': 'float64',
                'extra_time': 'float64',
                'grade_matches_current_submission': 'float64',
                'group_a': 'float64',
                'group_b': 'float64',
                'mellon_id': 'float64',
                'points_possible': 'float64',
                'quiz_fudge_points': 'float64',
                'quiz_id': 'float64',
                'quiz_points_possible': 'float64',
                'quiz_total_attempts': 'float64',
                'submission_id': 'float64',
                'time_taken': 'float64'})

            self.dfs.append(df)

    #used within class only, determines the size of applicable table & function
    def __count(self, table:int, f: Callable) -> dd:
        df = self.dfs[table]
        return df.loc[f].groupby(student_id).size()

    #3, counts assignment between a given date range; assignments file will be at index 2 in list
    def assignment_interval_count(self, start: str, end: str) -> dd:
        submitted_column = 'submitted_at'
        return self.__count(2, lambda df: (df[submitted_column] >= start) & (df[submitted_column] < end))

    #4, counts late assignments between a given date range; assignments file will be at index 2 in list
    def late_assignment_count(self) -> dd:
        submitted_column = 'submitted_at'
        due_column = 'due_at'
        return self.__count(2, lambda df: df[submitted_column] > df[due_column])

    #10, total number of submissions per student
    def assignment_submission_count(self) -> dd:
        df = self.dfs[2]
        return df.groupby(student_id).size()

    #15, avg student assignment score
    def assignment_average(self) -> dd:
        df = self.dfs[2]
        return df.groupby(student_id)['score'].mean()

    #20, avg number of attempts per quiz
    def avg_quiz_attempts(self) -> dd:
        df = self.dfs[3]
        quiz_attempts = "quiz_total_attempts"
        return df.groupby(student_id)[quiz_attempts].mean()

    #21, number of quizzes attempted per student
    def quizzes_attempted(self) -> dd:
        df = self.dfs[3]
        quiz_attempts = "quiz_total_attempts"
        return df.groupby(student_id)[quiz_attempts].sum()

    #23, number of quizzes passed/completed per student 
    def quizzes_passed(self) -> dd:
        correct = 'quiz_submission_kept_score'
        total = 'quiz_points_possible'
        return self.__count(3, lambda df: df[correct]/df[total] >= .735)

    #27, number of practice quizzes attempted per student
    def practice_quizzes_attempted(self) -> dd:
        df = self.dfs[3]
        t = "quiz_type"
        quiz_attempts = "quiz_total_attempts"
        return df.loc[lambda df: df[t] == "practice_quiz"].groupby(student_id)[quiz_attempts].sum()

    #30, total number of discussion forum posts per student
    def disc_post_count(self) -> dd:
        df = self.dfs[1]
        return df.groupby(student_id).size()

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

    """

    The requests table is unrelated to the others, therefore, the following functions are proof of concept for now.
    Mostly for Toy Data Request table

    #5, visits after deadline 
    def visits_after_deadline(self) -> int:
        req = self.dfs[0] # get requests table
        asgmt = self.dfs[1] # get assignments table
        deadlines = req[[student_id, 'due_at', 'url']]
        views = asgmt[[student_id, 'timestamp', 'url']]
    
        return count of views after deadline with matching urls for each student


    #6, visits before deadline 
    def visits_before_deadline(self) -> int:
        req = self.dfs[0] # get requests table
        asgmt = self.dfs[1] # get assignments table
        deadlines = req[[student_id, 'due_at', 'url']]
        views = asgmt[[student_id, 'timestamp', 'url']]
    
        return count of views before deadline with matching urls for each student
    
    #7, time between first assignment access and assignment due date
    def first_assignment_access(self) -> dd:
        req = self.dfs[0] # get requests table
        asgmt = self.dfs[1] # get assignments table
        first_access = req.loc[req.groupby([student_id, 'url'])['url'].idxmin()] # get student ids and the earliest urls for each assignment
        
        return assignments table grouped by student id, assignment info and the time between first assignment access and due date

    #31, number of discussion forum views
    def disc_views(self) -> dd:
        return self.__count(1, lambda df: df["action"] == "Read a reply in a discussion")

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