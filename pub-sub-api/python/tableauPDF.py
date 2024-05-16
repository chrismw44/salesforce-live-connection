import tableauserverclient as TSC
import requests
import pymupdf
import fitz
from credentials import *

# Set up Tableau Server connection
server_name = SERVER_NAME
site_id = SITE_ID
token_name = TOKEN_NAME
personal_access_token = PERSONAL_ACCESS_TOKEN

# Sign in using personal access token
tableau_auth = TSC.PersonalAccessTokenAuth( token_name = token_name, 
                                              personal_access_token = personal_access_token, 
                                              site_id = site_id )
server = TSC.Server(server_name, use_server_version=True)

server.auth.sign_in(tableau_auth)

# Get the workbook and view IDs
workbook_id = EMAIL_WORKBOOK_ID
view_ids = [TOTAL_ATTENDEES,
    TOTAL_MEETINGS,
    MEETING_ROOM_BOOKINGS,
    ATTENDEES_BY_COUNTRY,
    MEETINGS_BY_COUNTRY,
    ATTENDEES_BY_DATE,
    MEETINGS_BY_DATE,]

# Get the workbook and view objects by their IDs
workbook_item = server.workbooks.get_by_id(workbook_id)

# viewsheets = [f"{view.name.upper()} {view.id}" for view in workbook_item.views]
# print(viewsheets)

count = 0

for view_id in view_ids:
    view_item = server.views.get_by_id(view_id)

    image_req_option = TSC.ImageRequestOptions(imageresolution=TSC.ImageRequestOptions.Resolution.High, maxage=1)

    server.views.populate_image(view_item, image_req_option)
    with open(f'./{count}.png', 'wb') as f:
        f.write(view_item.image)
    count += 1

count = 0
images = ['0.png', '1.png', '2.png', '3.png', '4.png', '5.png', '6.png']

doc= fitz.open() # new, empty PDF 
for image in images: # read image file names 
    if image == '0.png' or image == '1.png' or image == '2.png':
        page = doc.new_page(width=842, height=595) # makes a new empty A4 page: (width=595, height=842) 
    else:
        page = doc.new_page(width=595, height=842)
    page.insert_image(page.rect, filename=image) 
doc.ez_save("drupa_meetings&attendees.pdf")