import tableauserverclient as TSC
from credentials import *
from refreshTableauData import *

def getWorkBooksTableauServer(token_name, personal_access_token, site_id, server_name):

    tableau_auth = TSC.PersonalAccessTokenAuth( token_name = token_name, 
                                              personal_access_token = personal_access_token, 
                                              site_id = site_id )
    server = TSC.Server(server_name, use_server_version=True)


    with server.auth.sign_in_with_personal_access_token(tableau_auth):
        print('[Logged in successfully]')
        print('[Loading workbooks...]')        
        all_workbooks_items, pagination_item = server.workbooks.get() 
        
        tableauWB = []
        for workbook in TSC.Pager(server.workbooks.get):
            tableauWB.append(
                ( workbook.id
                 ,workbook.name
                 ,workbook.created_at
                 ,workbook.updated_at  
                 ,workbook.webpage_url
            ))

        print('[Tableau {} workbooks loaded]'.format(len(tableauWB)))
        return tableauWB
    

tableauWB = getWorkBooksTableauServer(TOKEN_NAME, PERSONAL_ACCESS_TOKEN, SITE_ID, SERVER_NAME)
print(tableauWB)

