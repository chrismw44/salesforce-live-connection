#Event query
queryEvent = """
query EventsWithAccount {
  uiapi {
    query {
      Event(first: 2000,
        where: { 
          Objective_picklist__c: { like: "Drupa%" } 
          IsDeleted: {ne: true}
          StartDateTime: {
            gte: {value: "2024-05-28T00:00:00.000Z"},
            lt: {value: "2024-06-08T00:00:00.000Z"}
          }
        }
      ) 
      {
        edges {
          node {
            Id
            Subject {
              value
            }
            Objective_picklist__c {
              value
            }
            ProductField__c {
              value
            }
            Meeting_Room_Allocated__c {
              value
            }
            RecorderEmail__c {
              value
            }
            NumberofAttendees__c {
              value
            }
            Meeting_Room_Comments__c {
              value
            }
            MeetingCountry__c {
              value
            }
            Contacts__c {
              value
            }
            Event_Relation_IDs__c {
              value
            }
            FujifilmAttendees__c {
              value
            }
            PartnerAttendees__c {
              value
            }
            RejectedComment__c {
              value
            }
            StartDateTime {
              value
            }
            EndDateTime {
              value
            }
            Account {
              Id
              Name {
                value
              }
            }
            Owner {
              ...on User {
                Id
                Name {
                  value
                }
                Email {
                  value
                }
              }
            }
          }
        }
      }
    }
  }
}
"""


#EventContacts query
queryEventContacts = """
query EventContacts ($relationIDs: [ID]) {
  uiapi {
    query {
      Contact (first: 2000, 
        where: { 
          Id: { in: $relationIDs }
          } 
        ) 
        {
        edges {
          node {
            Id
            Full_Name__c {
              value
            }
            Email {
              value
            }
            Mobile__c {
              value
            }
            Primary_Language__c {
              value
            }
            Title {
                value
            }
            Job_Title__r {
              Name {
                value
              }
            }
          }
        }
      }
    }
  }
}

"""



#EventColleague query
queryEventColleague = """
query EventColleague {
  uiapi {
    query {
      Event_Colleague__c (first: 2000,
        where: { 
          IsDeleted: {ne: true}
        }
      ) 
      {
        edges {
          node {
            Id
            Comments__c {
              value
            }
            DatesAttending__c {
              value
            }
            EmailAddress__c {
              value
            }
            EventName__c {
              value
            }
            ExhibitorCode__c {
              value
            }
            FirstName__c {
              value
            }
            LastName__c {
              value
            }
            Lunch_Required__c {
              value
            }
            HotelRoom__c {
              value
            }
            JobTitle__c {
              value
            }
            Lang__c {
              value
            }
            MobilePhone__c {
              value
            }
            MobilePhoneClean__c {
              value
            }
            Country__c {
              value
            }
            NoOfPolos__c {
              value
            }
            PoloSize__c {
              value
            }
            Primary_Language__c {
              value
            }
            ProductSpeciality__c {
              value
            }
            StafforVisitor__c {
              value
            }
            Uniform__c {
              value
            }
          }
        }
      }
    }
  }
}

"""


