### Data Processing in R and Python 2022Z
### Homework Assignment no. 2
###
### IMPORTANT
### This file should contain only solutions to tasks in the form of a functions
### definitions and comments to the code.
###
#
import pandas as pd
import numpy as np

# -----------------------------------------------------------------------------#
# Task 1
# -----------------------------------------------------------------------------#

def solution_1(Posts):
    result = Posts
    result['Year'] = result['CreationDate']
    result['Year'] = pd.to_datetime(result['Year'], errors='coerce').dt.strftime('%Y')
    result = result.groupby('Year', as_index=False)['Id'].count().rename(columns={"Id": "TotalNumber"})
    return result


# -----------------------------------------------------------------------------#
# Task 2
# -----------------------------------------------------------------------------#

def solution_2(Users, Posts):
    us = Users[['Id', 'DisplayName']]
    questions = Posts[Posts['PostTypeId'] == 1][['OwnerUserId', 'ViewCount']]
    result = pd.merge(left=us, right=questions, left_on='Id', right_on='OwnerUserId')[
        ['Id', 'DisplayName', 'ViewCount']].groupby(['Id', 'DisplayName'], as_index=False)['ViewCount'].sum().rename(
        columns={"ViewCount": "TotalViews"}).sort_values(by='TotalViews', ascending=False).head(10)
    result.index = range(0, len(result.index))
    return result


# -----------------------------------------------------------------------------#
# Task 3
# -----------------------------------------------------------------------------#

def solution_3(Badges):
    bn = Badges[['Name', 'Date']]
    BadgesNames = bn.loc[:].copy()
    BadgesNames.loc[:, 'Year'] = BadgesNames.apply(lambda row: pd.to_datetime(row.Date, errors='coerce').strftime('%Y'),
                                                   axis=1)
    BadgesNames = BadgesNames[['Name', 'Year']].groupby(['Name', 'Year']).size().reset_index(name="Count")

    BadgesYearly = BadgesNames.groupby('Year')['Count'].sum().reset_index(name="CountTotal")

    NoName = pd.merge(left=BadgesNames, right=BadgesYearly, left_on='Year', right_on='Year')
    NameAndCount = NoName[['Name', 'Count', 'Year']]
    NoName = NoName.groupby(['Year', 'CountTotal'], as_index=False)['Count'].max()
    result = pd.merge(left=NoName, right=NameAndCount, left_on=['Year', 'Count'], right_on=['Year', 'Count'],
                      how='left')
    result.loc[:, 'MaxPercentage'] = result.apply(lambda row: row.Count / row.CountTotal, axis=1)
    result = result[['Year', 'Name', 'MaxPercentage']]
    return result


# -----------------------------------------------------------------------------#
# Task 4
# -----------------------------------------------------------------------------#

def solution_4(Comments, Posts, Users):
    CmtTotScr = Comments[['PostId', 'Score']].groupby('PostId', as_index=False)['Score'].sum().rename(
        columns={"Score": "CommentsTotalScore"})
    PostsBestComments = pd.merge(left=CmtTotScr, right=Posts, left_on="PostId", right_on="Id")
    PostsBestComments = PostsBestComments[PostsBestComments['PostTypeId'] == 1][
        ['OwnerUserId', 'Title', 'CommentCount', 'ViewCount', 'CommentsTotalScore']]
    PostsBestComments.index = range(0, len(PostsBestComments.index))
    result = pd.merge(left=PostsBestComments, right=Users, left_on="OwnerUserId", right_on="Id")[
        ['Title', 'CommentCount', 'ViewCount', 'CommentsTotalScore', 'DisplayName', 'Reputation',
         'Location']].sort_values(by='CommentsTotalScore', ascending=False).head(10)
    result.index = range(0, len(result.index))
    result = result.where(pd.notnull(result), None)
    return result


# -----------------------------------------------------------------------------#
# Task 5
# -----------------------------------------------------------------------------#

def solution_5(Posts, Votes):
    VotesDates = Votes[Votes['VoteTypeId'].isin([3, 4, 12])][['PostId', 'CreationDate']]
    VotesDates.loc[:, 'Year'] = VotesDates.apply(
        lambda row: pd.to_datetime(row.CreationDate, errors='coerce').strftime('%Y'), axis=1)
    VotesDates.loc[:, 'VoteDate'] = VotesDates.apply(lambda row: np.where(int(row.Year) == 2022, 'after',
                                                                          np.where(int(row.Year) == 2021, 'during',
                                                                                   np.where(int(row.Year) == 2020,
                                                                                            'during', np.where(
                                                                                           int(row.Year) == 2019,
                                                                                           'during', 'before')))),
                                                     axis=1)
    VotesDates = VotesDates[['PostId', 'VoteDate']]
    VotesDates['VoteDate'] = VotesDates['VoteDate'].astype('string')
    VotesDates = VotesDates.groupby(['PostId', 'VoteDate']).size().reset_index(name="Total")
    VotesByAge = VotesDates
    VotesByAge['BeforeCOVIDVotes'] = VotesByAge.apply(lambda row: np.where(row.VoteDate == 'before', row.Total, 0),
                                                      axis=1).astype('int64')
    VotesByAge['DuringCOVIDVotes'] = VotesByAge.apply(lambda row: np.where(row.VoteDate == 'during', row.Total, 0),
                                                      axis=1).astype('int64')
    VotesByAge['AfterCOVIDVotes'] = VotesByAge.apply(lambda row: np.where(row.VoteDate == 'after', row.Total, 0),
                                                     axis=1).astype('int64')
    VotesByAge = VotesByAge.groupby('PostId', as_index=False)[
        ['BeforeCOVIDVotes', 'DuringCOVIDVotes', 'AfterCOVIDVotes']].max()
    VotesByAge['Votes'] = VotesByAge.apply(
        lambda row: row.BeforeCOVIDVotes + row.DuringCOVIDVotes + row.AfterCOVIDVotes, axis=1)
    result = pd.merge(left=Posts, right=VotesByAge, left_on="Id", right_on="PostId")
    result = result[(~(result['Title'].isnull())) & (result['DuringCOVIDVotes'] > 0)][
        ['Title', 'CreationDate', 'PostId', 'BeforeCOVIDVotes', 'DuringCOVIDVotes', 'AfterCOVIDVotes',
         'Votes']].sort_values(by=['DuringCOVIDVotes', 'Votes'], ascending=[False, False]).head(20)
    result.loc[:, 'Date'] = result.apply(
        lambda row: pd.to_datetime(row.CreationDate, errors='coerce').strftime('%Y-%m-%d'), axis=1)
    result = result[['Title', 'Date', 'PostId', 'BeforeCOVIDVotes', 'DuringCOVIDVotes', 'AfterCOVIDVotes', 'Votes']]
    result.index = range(0, len(result.index))
    return result
