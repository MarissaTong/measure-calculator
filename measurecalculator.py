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

    #30, total number of discussion forum posts per student
    def disc_post_count(self) -> dd:
        df = self.dfs[1]
        return df.groupby(student_id).size()


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
    

    #38, quality of discussion post 
    def quality_discpost(self) -> dd:
        df = self.dfs[1]
        return df[[student_id,'depth']][df['parent_discussion_entry_id'].isnull()] 


    #39, quality of discussion reply
    def quality_discreply(self) -> dd:
        df = self.dfs[1]
        df = df.dropna(subset=['parent_discussion_entry_id'])
        return df[[student_id,'depth']]

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


    #32, # replies to forums posts 
    def replies_posts(self) -> dd:
        return self.__count(0, lambda df: df['action'] == 'Click "Post Reply" in a discussion')


    #33, time spent posting messages on discussion board
    def time_post_disc(self) -> dd:
        


    #34, time spent replying to forum posts
    def time_reply_disc(self) ->dd:
        req = self.dfs[0] #requests table
        disc = self.dfs[1] #discussion table

        req = req[[student_id,'timestamp','url','action']][req['action'] == 'Click "Reply" to an entry(reply) in a discussion' | req['action'] == 'Click "Post Reply" in a discussion']
        #get the time stemp from clicking reply to clicking post to calculate time spent according to student id

        return table corresponding student id, message & duration of time taken to post



    #35, time spent viewing discussions
    def time_view_disc(self) -> dd:
        req = self.dfs[0] #requests table
        req = req[[student_id,'timestamp','action']][req['action'] == 'Click on a discussion post' | req['action'] == 'Click a discussion (api)']
        #calculate how long time was spent viewing discussion page

        return table corresponding to student id and length of time viewing discussion


    #40, # of course calendar views
    def calendar_views(self) -> dd:
        return self.__count(0, lambda df: df['action'] == 'Click on "View Course Calendar"')
    

    #41, time spent on course home page
    def time_homepage(self) -> dd:
        req = self.dfs[0] #requests table
        l = ['Click on "View Course Stream"','Click "Home" on the left panel','Click on "View Course Calendar"', 'Click on "View Course Notifications"', 'Go on To-Do Item']
        req = req[[student_id, 'timestamp']][req['action] = lambda x: x, l]

        return table organized by student id and calculated time spent on course page


    #42, # of course syllabus views
    def syllabus(self) -> dd:
        return self.__count(0, lambda df: df['action'] == 'Click on "Syllabus" on the left panel' | df['action'] == 'Click the course link that the students take (enter the syllabus page) in people page')


    #45, visits to the gradebook (toyrequest sheet)
    def visits_gradebook(self) -> dd:
        return self.__count(0, lambda df: df['action'] == 'Click on "Grades"')


    #46, clicks on instructional content (toyrequest sheet)
    def clicks_instrucContent(self) -> dd:
        df = self.dfs[0]
        df = df[[student_id,'web_application_controller']][(df['web_application_controller'] == 'files') | (df['web_application_controller'] == 'context_modules') ]

        return requests table grouped by student id and use the size function (get the # of times EACH student clicked anything related to instructional content) 


   #48, files clicked/viewed (toyrequest sheet)
    def file_count(self) -> dd:
        return self.__count(0, lambda df: df["action"] == "Click on / View a specific file" | df["action"] == "view a file in files")


    #53, # accesses to lecture videos
    def lecture_vid_access(self) -> dd:
        return self.__count(0,lambda df: df['action'] == 'Click on a recording (Specify Recording)')


    #56, # study sessions
    def study_sessions(self) ->dd:
        req = self.dfs[0]
        return req[[student_id,'session_id']].groupby(student_id).size()


    #57, # study sessions (first 8 weeks)
    def study_sessions_8(self) -> dd:
        req = self.dfs[0]
        req = req[[student_id,'session_id','timestamp']][req['timestamp'] < #get earliest time and add 8 weeks to it]

        return req.groupby(student_id).size()


    #61, # of clicks total (toyrequest sheet) ?
    def clicks(self) -> dd:
        return self.__count(0, lambda df: if "click" in df["action"].lower())


    #62, time spent online -> dd:
        req = self.dfs[0]
        disc = self.dfs[1]
        asmgt = self.dfs[2]
        
        return new dd with student Id and corresponding time spent online 
    """


if __name__ == "__main__":
    mc = MeasureCalculator()
    mc.read_files(files)

    df = mc.study_sessions()
    print(df)

    df.to_csv(['csvFiles/random_testing.csv'])