import requests
import pandas as pd
import numpy as np
import re
import pantab
import subprocess
import json
import tableauserverclient as TSC
from credentials import *
from datetime import datetime
from errorLogging import *


#SF GET ACCESS TOKEN
def get_salesforce_access_token(username):
    # Run the Salesforce CLI command to get the org info in JSON format
    command = f"sf org display --target-org {username} --json"
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()

    # Check for errors
    if process.returncode != 0:
        raise Exception(f"Error running command: {error.decode('utf-8')}")

    # Parse the JSON output and extract the access token
    sf_cli_info = json.loads(output.decode('utf-8'))
    access_token = sf_cli_info['result']['accessToken']

    return access_token

        


#EVENT DATA REQUEST
def eventDataRequest(endpoint, queryE, access_token):
  # Make the request
  responseE = requests.post(endpoint, json={'query': queryE}, headers={'Authorization': f'Bearer {access_token}'})

  # Check for successful response
  if responseE.status_code == 200:
      dataE = responseE.json()
      events_data = dataE['data']['uiapi']['query']['Event']['edges']

      # Process data
      formatted_dataE = []
      for event in events_data:
          node = event['node']
          formatted_event = {
            'Activity ID': node['Id'],
            'Subject': node['Subject']['value'] if 'Subject' in node else None,
            'Objective1': node['Objective_picklist__c']['value'] if 'Objective_picklist__c' in node else None,
            'Product Field': node['ProductField__c']['value'] if 'ProductField__c' in node else None,
            'Meeting Room Allocated': node['Meeting_Room_Allocated__c']['value'] if 'Meeting_Room_Allocated__c' in node else None,
            'Recorder Email': node['RecorderEmail__c']['value'] if 'RecorderEmail__c' in node else None,
            'Number of Attendees': node['NumberofAttendees__c']['value'] if 'NumberofAttendees__c' in node else None,
            'Meeting Room Comments': node['Meeting_Room_Comments__c']['value'] if 'Meeting_Room_Comments__c' in node else None,
            'Country': node['MeetingCountry__c']['value'] if 'MeetingCountry__c' in node else None,
            'Contacts': node['Contacts__c']['value'] if 'Contacts__c' in node else None,
            'Fujifilm Attendees': node['FujifilmAttendees__c']['value'] if 'FujifilmAttendees__c' in node else None,
            'Partner Attendees': node['PartnerAttendees__c']['value'] if 'PartnerAttendees__c' in node else None,
            'Rejected Comment': node['RejectedComment__c']['value'] if 'RejectedComment__c' in node else None,
            'Start Date Time': node['StartDateTime']['value'] if 'StartDateTime' in node else None,
            'End Date Time': node['EndDateTime']['value'] if 'EndDateTime' in node else None,
            'Account ID': node['Account']['Id'] if 'Account' in node and 'Id' in node['Account'] else None,
            'Account Name': node['Account']['Name']['value'] if 'Account' in node and 'Name' in node['Account'] else None,
            'Assigned To ID': node['Owner']['Id'] if 'Owner' in node and 'Id' in node['Owner'] else None,
            'Event Relation IDs': node['Event_Relation_IDs__c']['value'] if 'Event_Relation_IDs__c' in node else None,
            'User Name': node['Owner']['Name']['value'] if 'Owner' in node and 'Name' in node['Owner'] else None,
            'User Email': node['Owner']['Email']['value'] if 'Owner' in node and 'Email' in node['Owner'] else None,
          }

          formatted_dataE.append(formatted_event)

      # Create DataFrame
      dfE = pd.DataFrame(formatted_dataE)
      print("1/5: Event data successfully fetched and stored in DataFrame")
      return dfE
  
  else:
      raise Exception("Error gathering Event data:", responseE.text)


#CONTACT DATA REQUEST
def eventContactDataRequest(endpoint, queryECon, relationIDs, access_token):

  # Make the request
  responseC = requests.post(endpoint, json={'query': queryECon, 'variables': {'relationIDs': relationIDs}}, headers={'Authorization': f'Bearer {access_token}'})

  # Check for successful response
  if responseC.status_code == 200:
      dataC = responseC.json()
      contacts_data = dataC['data']['uiapi']['query']['Contact']['edges']

      # Process data
      formatted_dataC = []
      for contact in contacts_data:
          node = contact['node']
          formatted_contact = {
              'Event Relation IDs Split': node['Id'],
              'Contact Full Name': node['Full_Name__c']['value'] if 'Full_Name__c' in node else None,
              'Email Address': node['Email']['value'] if 'Email' in node else None,
              'Mobile Phone': node['Mobile__c']['value'] if 'Mobile__c' in node else None,
              'Primary Language': node['Primary_Language__c']['value'] if 'Primary_Language__c' in node else None,
              'Contact Title': node['Title']['value'] if 'Title' in node else None,
              'JobTitle': None
          }
          if 'Job_Title__r' in node and node['Job_Title__r'] is not None:
              job_title = node['Job_Title__r']
              formatted_contact.update({
                  'JobTitle': job_title['Name']['value'] if job_title and 'Name' in job_title else None
          })
          formatted_dataC.append(formatted_contact)

      # Create DataFrame
      dfC = pd.DataFrame(formatted_dataC)
      dfC['JobTitle'] = dfC['JobTitle'].fillna(dfC['Contact Title']) 
      dfC = dfC.drop(['Contact Title'], axis=1)
      print("2/5: Contact data successfully fetched and stored in DataFrame")
      return dfC

  else:
      raise Exception("Error gathering Contact data:", responseC.text)




#EVENTCOLLEAGUE DATA REQUEST
def eventColleagueDataRequest(endpoint, queryEC, access_token):

  # Make the request
  responseEC = requests.post(endpoint, json={'query': queryEC}, headers={'Authorization': f'Bearer {access_token}'})

  # Check for successful response
  if responseEC.status_code == 200:
      dataEC = responseEC.json()
      event_colleague_data = dataEC['data']['uiapi']['query']['Event_Colleague__c']['edges']

      # Process data
      formatted_dataEC = []
      for event_colleague in event_colleague_data:
          node = event_colleague['node']
          formatted_event_colleague = {
							'Record ID_Attendees': node['Id'] if 'Id' in node else None,
							'Comments': node['Comments__c']['value'] if 'Comments__c' in node else None,
							'Dates Attending': node['DatesAttending__c']['value'] if 'DatesAttending__c' in node else None,
							'Email Address': node['EmailAddress__c']['value'] if 'EmailAddress__c' in node else None,
							'Event Name': node['EventName__c']['value'] if 'EventName__c' in node else None,
							'Exhibitor Code': node['ExhibitorCode__c']['value'] if 'ExhibitorCode__c' in node else None,
							'First Name': node['FirstName__c']['value'] if 'FirstName__c' in node else None,
							'Last Name': node['LastName__c']['value'] if 'LastName__c' in node else None,
              'Lunch Required' : node['Lunch_Required__c']['value'] if 'Lunch_Required__c' in node else None,
							'Hotel Room': node['HotelRoom__c']['value'] if 'HotelRoom__c' in node else None,
							'JobTitle': node['JobTitle__c']['value'] if 'JobTitle__c' in node else None,
							'Languages Spoken': node['Lang__c']['value'] if 'Lang__c' in node else None,
							'Mobile Phone': node['MobilePhone__c']['value'] if 'MobilePhone__c' in node else None,
              'Mobile Phone Clean': node['MobilePhoneClean__c']['value'] if 'MobilePhoneClean__c' in node else None,
							'Country': node['Country__c']['value'] if 'Country__c' in node else None,
							'No of Polos': node['NoOfPolos__c']['value'] if 'NoOfPolos__c' in node else None,
							'Polo Size': node['PoloSize__c']['value'] if 'PoloSize__c' in node else None,
							'Primary Language': node['Primary_Language__c']['value'] if 'Primary_Language__c' in node else None,
							'Product Speciality': node['ProductSpeciality__c']['value'] if 'ProductSpeciality__c' in node else None,
							'Staff or Visitor': node['StafforVisitor__c']['value'] if 'StafforVisitor__c' in node else None,
							'Uniform': node['Uniform__c']['value'] if 'Uniform__c' in node else None,
					}
          formatted_dataEC.append(formatted_event_colleague)

      # Create DataFrame
      dfEC = pd.DataFrame(formatted_dataEC)
      print("3/5: Event Colleague data successfully fetched and stored in DataFrame")
      return dfEC

  else:
      raise Exception("Error gathering Event Colleague data:", responseEC.text)




#EVENT DATA TRANSFORMATION
def eventTransform(dfTransformE):
  if dfTransformE is not None:
    if not dfTransformE.empty:
      #Get Requested By from Recorder Email
      dfTransformE['Requested By'] = dfTransformE['Recorder Email'].apply(lambda x: x.split('@')[0] if pd.notnull(x) else None)
      dfTransformE['Requested By'] = dfTransformE['Requested By'].apply(lambda x: re.sub(r'[^a-zA-Z]', ' ', x) if pd.notnull(x) else None)
      dfTransformE['Requested By'] = dfTransformE['Requested By'].apply(lambda x: x.title() if pd.notnull(x) else None)


      # Set Event date columns to datetime
      dfTransformE['Start Date Time'] = pd.to_datetime(dfTransformE['Start Date Time'])
      dfTransformE['End Date Time'] = pd.to_datetime(dfTransformE['End Date Time'])

      # Function to map date to string
      def map_date_to_string(date):
          date_string = date.strftime("%B %d - %A")
          return date_string

      # Function to generate list of dates between start and end dates
      def generate_date_range(start_date, end_date):
          return [start_date + pd.Timedelta(days=x) for x in range((end_date - start_date).days + 1)]

      # Function to map list of dates to string
      def map_date_range_to_string(date_list):
          return ', '.join(map(map_date_to_string, date_list))

      # Apply functions to create new columns
      dfTransformE['Meeting Start Date'] = dfTransformE['Start Date Time'].apply(map_date_to_string)
      dfTransformE['Meeting End Date'] = dfTransformE['End Date Time'].apply(map_date_to_string)
      dfTransformE['Meeting Dates'] = dfTransformE.apply(lambda row: map_date_range_to_string(generate_date_range(row['Start Date Time'], row['End Date Time'])), axis=1)


      #Make columns comma separated
      dfTransformE['Product Field'] = dfTransformE['Product Field'].str.replace(";", ", ")
      dfTransformE['Contacts'] = dfTransformE['Contacts'].str.replace(",", ", ")
      dfTransformE['Contacts'] = dfTransformE['Contacts'].str.replace(",  ", ", ")
      dfTransformE['Fujifilm Attendees'] = dfTransformE['Fujifilm Attendees'].str.replace(",", ", ")
      dfTransformE['Fujifilm Attendees'] = dfTransformE['Fujifilm Attendees'].str.replace(",  ", ", ")
      dfTransformE['Partner Attendees'] = dfTransformE['Partner Attendees'].str.replace(",", ", ")
      dfTransformE['Partner Attendees'] = dfTransformE['Partner Attendees'].str.replace(",  ", ", ")


      #Duplicate fields for splitting
      dfTransformE['ProductFieldSplit'] = dfTransformE.loc[:, 'Product Field']
      dfTransformE['ContactsSplit'] = dfTransformE.loc[:, 'Contacts']
      dfTransformE['FujifilmAttendeesSplit'] = dfTransformE.loc[:, 'Fujifilm Attendees']
      dfTransformE['PartnerAttendeesSplit'] = dfTransformE.loc[:, 'Partner Attendees']
      dfTransformE['EventRelationIDsSplit'] = dfTransformE.loc[:, 'Event Relation IDs']
      dfTransformE['MeetingDatesSplit'] = dfTransformE.loc[:, 'Meeting Dates']
      dfTransformE['CountrySplit'] = dfTransformE.loc[:, 'Country']

      #Split and pivot product and contact fields
      dfTransformE = dfTransformE.assign(ProductFieldSplit=dfTransformE.ProductFieldSplit.str.split(", ")).explode("ProductFieldSplit")
      dfTransformE.rename(columns={"ProductFieldSplit": "Product Field Split"}, inplace=True)

      dfTransformE = dfTransformE.assign(ContactsSplit=dfTransformE.ContactsSplit.str.split(", ")).explode("ContactsSplit")
      dfTransformE.rename(columns={"ContactsSplit": "Contacts Split"}, inplace=True)

      dfTransformE = dfTransformE.assign(FujifilmAttendeesSplit=dfTransformE.FujifilmAttendeesSplit.str.split(", ")).explode("FujifilmAttendeesSplit")
      dfTransformE.rename(columns={"FujifilmAttendeesSplit": "Fujifilm Attendees Split"}, inplace=True)

      dfTransformE = dfTransformE.assign(PartnerAttendeesSplit=dfTransformE.PartnerAttendeesSplit.str.split(", ")).explode("PartnerAttendeesSplit")
      dfTransformE.rename(columns={"PartnerAttendeesSplit": "Partner Attendees Split"}, inplace=True)

      dfTransformE = dfTransformE.assign(EventRelationIDsSplit=dfTransformE.EventRelationIDsSplit.str.split(",")).explode("EventRelationIDsSplit")
      dfTransformE.rename(columns={"EventRelationIDsSplit": "Event Relation IDs Split"}, inplace=True)

      dfTransformE = dfTransformE.assign(MeetingDatesSplit=dfTransformE.MeetingDatesSplit.str.split(", ")).explode("MeetingDatesSplit")
      dfTransformE.rename(columns={"MeetingDatesSplit": "Meeting Dates Split"}, inplace=True)
      dfTransformE['Dates Attending Split'] = dfTransformE.loc[:, 'Meeting Dates Split']

      dfTransformE = dfTransformE.assign(CountrySplit=dfTransformE.CountrySplit.str.split(", ")).explode("CountrySplit")
      dfTransformE.rename(columns={"CountrySplit": "Country Split"}, inplace=True)

      #Create new merged columns for Company, Recorder Email and Requested By
      dfTransformE['Company'] = np.where(dfTransformE['Account ID'] == '0016700006EtLL3AAN', dfTransformE['Subject'], dfTransformE['Account Name'])
      dfTransformE['Recorder Email'] = dfTransformE['Recorder Email'].fillna(dfTransformE['User Email']) 
      dfTransformE['Requested By'] = dfTransformE['Requested By'].fillna(dfTransformE['User Name']) 
   
  return dfTransformE


#GROUP SF CONTACTS FOR EACH ACTIVITY
def groupContacts(dfTransformE):
    if dfTransformE is not None:
      if not dfTransformE.empty:
        dfGroupedContacts = dfTransformE[['Activity ID', 'Contact Full Name']].dropna()
        dfGroupedContacts = dfGroupedContacts.rename(columns={'Contact Full Name': 'SF Contacts'})
        dfGroupedContacts = dfGroupedContacts.groupby("Activity ID",as_index=False).agg(lambda x: ', '.join(set(x.tolist())))
        dfTransformE = pd.merge(dfTransformE, dfGroupedContacts, on="Activity ID", how="left")
        dfTransformE['Contacts'] = dfTransformE['Contacts'].fillna(dfTransformE['SF Contacts']) 
        dfTransformE['Name'] = dfTransformE['Contacts Split'].fillna(dfTransformE['Contact Full Name']) 
        dfTransformE = dfTransformE.drop(['SF Contacts', 'Contact Full Name'], axis=1)

    return dfTransformE

#GROUP DATES ATTENDING FOR EACH CONTACT
def groupDatesAttending(dfTransformE):
    if dfTransformE is not None:
      if not dfTransformE.empty:
        dfTransformE['NameCompany'] = dfTransformE['Name'] + dfTransformE['Company']
        dfTransformE['NameCompany'] = dfTransformE['NameCompany'].str.replace(" ", "")
        dfGroupedDatesAttending = dfTransformE[['NameCompany', 'Dates Attending Split']].dropna()
        dfGroupedDatesAttending = dfGroupedDatesAttending.rename(columns={'Dates Attending Split': 'Dates Attending'})
        dfGroupedDatesAttending = dfGroupedDatesAttending.groupby("NameCompany",as_index=False).agg(lambda x: ', '.join(set(x)))
        # Function to sort dates within each row
        def sort_dates(row):
            # Split the comma-separated dates
            dates = row.split(', ')
            # Convert to datetime objects
            date_objs = [datetime.strptime(date.split(' - ')[0], '%B %d') for date in dates]
            # Sort the datetime objects
            sorted_dates = sorted(date_objs)
            # Convert back to string format
            sorted_dates_str = [date.strftime('%B %d - %A') for date in sorted_dates]
            # Join the sorted dates
            return ', '.join(sorted_dates_str)

        # Apply the function to each row in the 'dates' column
        dfGroupedDatesAttending['Dates Attending'] = dfGroupedDatesAttending['Dates Attending'].apply(sort_dates)
        dfTransformE = pd.merge(dfTransformE, dfGroupedDatesAttending, on="NameCompany", how="left")

    return dfTransformE
   

#EVENTCOLLEAGUE DATA TRANSFORMATION
def eventColleagueTransform(dfTransformEC):
  if dfTransformEC is not None:
    if not dfTransformEC.empty:

      #Make comma separated
      dfTransformEC['Dates Attending'] = dfTransformEC['Dates Attending'].str.replace(";", ", ")
      dfTransformEC['Languages Spoken'] = dfTransformEC['Languages Spoken'].str.replace(";", ", ")
      dfTransformEC['Product Speciality'] = dfTransformEC['Product Speciality'].str.replace(";", ", ")

      #Duplicate fields for splitting
      dfTransformEC['DatesAttendingSplit'] = dfTransformEC.loc[:, 'Dates Attending']
      dfTransformEC['LanguagesSpokenSplit'] = dfTransformEC.loc[:, 'Languages Spoken']
      dfTransformEC['ProductSpecialitySplit'] = dfTransformEC.loc[:, 'Product Speciality']
      dfTransformEC['Country Split'] = dfTransformEC.loc[:, 'Country']

      #Split and pivot date, language and product fields
      dfTransformEC = dfTransformEC.assign(DatesAttendingSplit=dfTransformEC.DatesAttendingSplit.str.split(", ")).explode("DatesAttendingSplit")
      dfTransformEC.rename(columns={"DatesAttendingSplit": "Dates Attending Split"}, inplace=True)

      dfTransformEC = dfTransformEC.assign(LanguagesSpokenSplit=dfTransformEC.LanguagesSpokenSplit.str.split(", ")).explode("LanguagesSpokenSplit")
      dfTransformEC.rename(columns={"LanguagesSpokenSplit": "Languages Spoken Split"}, inplace=True)

      dfTransformEC = dfTransformEC.assign(ProductSpecialitySplit=dfTransformEC.ProductSpecialitySplit.str.split(", ")).explode("ProductSpecialitySplit")
      dfTransformEC.rename(columns={"ProductSpecialitySplit": "Product Speciality Split"}, inplace=True)

      #Add company column with the value 'Fujifilm'
      dfTransformEC['Company']='Fujifilm'

      #Create a full name column
      dfTransformEC['Name'] = dfTransformEC['First Name'] + ' ' + dfTransformEC['Last Name']

      dfTransformEC['NameCompany'] = dfTransformEC['Name'] + dfTransformEC['Company']
      dfTransformEC['NameCompany'] = dfTransformEC['NameCompany'].str.replace(" ", "")

  return dfTransformEC
  


#Define function to concatenate Event and Event Colleague
def joinedDataFrame(dfTransformE, dfTransformEC):

  dfAttendanceMeeting = pd.concat([dfTransformEC, dfTransformE])

  if dfTransformE.empty and not dfTransformEC.empty:
    dfAttendanceMeeting['Record ID'] = dfAttendanceMeeting['Record ID_Attendees']
  elif dfTransformEC.empty and not dfTransformE.empty:
    dfAttendanceMeeting['Record ID'] = dfAttendanceMeeting['Activity ID']
  else:
    dfAttendanceMeeting['Record ID'] = dfAttendanceMeeting['Record ID_Attendees'].fillna(dfAttendanceMeeting['Activity ID']) 

  if not dfAttendanceMeeting.empty:
    dfAttendanceMeeting['Languages Spoken'] = dfAttendanceMeeting['Languages Spoken'].fillna(dfAttendanceMeeting['Primary Language'])   

    dfAttendanceMeeting['Start Date Time'] = dfAttendanceMeeting['Start Date Time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    dfAttendanceMeeting['Start Date Time'] = pd.to_datetime(dfAttendanceMeeting['Start Date Time'])
    dfAttendanceMeeting['Start Date Time'] += pd.Timedelta(hours=2)
    dfAttendanceMeeting['End Date Time'] = dfAttendanceMeeting['End Date Time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    dfAttendanceMeeting['End Date Time'] = pd.to_datetime(dfAttendanceMeeting['End Date Time'])
    dfAttendanceMeeting['End Date Time'] += pd.Timedelta(hours=2)
  
    dfAttendanceMeeting['Company'] = dfAttendanceMeeting['Company'].str.replace("&amp;", "&")
    dfAttendanceMeeting['Company'] = dfAttendanceMeeting['Company'].str.replace("&#39;", "'")
    dfAttendanceMeeting.loc[dfAttendanceMeeting['Objective1'] == 'Drupa Meeting', 'Meeting Room Allocated'] = 'Not Requested'



  for feature in dfAttendanceMeeting:
      if dfAttendanceMeeting[feature].dtype == 'object':
          dfAttendanceMeeting[feature] = dfAttendanceMeeting[feature].astype('string[pyarrow]')

  return dfAttendanceMeeting




# Push the hyper files to Tableau Server as Published DataSources so they can be accessed from there straight  

#Define the Tableau Server datasource refresh function
def publishToServer(token_name, personal_access_token, site_id, server_name, project_name, sfDrupaLive):

  # Sign in to server
  tableau_auth = TSC.PersonalAccessTokenAuth( token_name = token_name, 
                                              personal_access_token = personal_access_token, 
                                              site_id = site_id )
  server = TSC.Server(server_name, use_server_version=True)

  print(f"4/5: Signing into Tableau Server and publishing based on " + sfDrupaLive)

  with server.auth.sign_in(tableau_auth):
      # Get project_id from project_name
      all_projects, pagination_item = server.projects.get()
      for project in TSC.Pager(server.projects):
          # Specify the project where you want the datasource to be published
          if project.name == project_name:
              project_id = project.id

      # Create the datasource object with the project_id
      new_datasource = TSC.DatasourceItem(project_id)

      # Publish datasource (here based on the first hyper file we created: data_science_info.hyper stored in hyper_filename_1)
      try:
          datasource = server.datasources.publish(new_datasource, sfDrupaLive,  mode = 'Overwrite')
          print("5/5: Datasource published. Datasource ID: {0}".format(datasource.id))
          message = "Datasource " + sfDrupaLive + " published. Datasource ID: {0}".format(datasource.id) + " successfull\n"
      except:
          message = sfDrupaLive + " FAILED\n"


#RUN ALL DATA FUNCTIONS
def process_event_data(ENDPOINT, queryEvent, queryEventContacts, queryEventColleague, ACCESS_TOKEN):
    # Retrieve event data
    dfDataE = eventDataRequest(ENDPOINT, queryEvent, ACCESS_TOKEN)
    
    # Transform event data
    dfTransformE = eventTransform(dfDataE)
    
    # Get relation IDs
    relationIDs = dfTransformE['Event Relation IDs Split'].dropna().tolist()
    
    # Retrieve event contact data
    dfDataC = eventContactDataRequest(ENDPOINT, queryEventContacts, relationIDs, ACCESS_TOKEN)
    
    # Merge event data with contact data
    dfTransformE = pd.merge(dfTransformE, dfDataC, on="Event Relation IDs Split", how="left")
    
    # Group contacts
    dfTransformE = groupContacts(dfTransformE)
    
    # Group dates attending
    dfTransformE = groupDatesAttending(dfTransformE)
    
    # Retrieve event colleague data
    dfDataEC = eventColleagueDataRequest(ENDPOINT, queryEventColleague, ACCESS_TOKEN)
    
    # Transform event colleague data
    dfTransformEC = eventColleagueTransform(dfDataEC)
    
    # Join event data with event colleague data
    dfAttendanceMeeting = joinedDataFrame(dfTransformE, dfTransformEC)
    
    # Convert DataFrame to Hyper file
    sfDrupaLive = "salesforce_drupa_live.hyper"
    pantab.frame_to_hyper(dfAttendanceMeeting, sfDrupaLive, table="sfDrupaLive")
    
    # Publish to server
    publishToServer(TOKEN_NAME, PERSONAL_ACCESS_TOKEN, SITE_ID, SERVER_NAME, PROJECT_NAME, sfDrupaLive)



