from modules import *
from enum import Enum, unique
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData


@unique
class CalculatedFields(Enum):
    Total_Account_ForBarring = 1
    Total_Accounts_ForTermination = 2
    Count_ActiveAccounts = 3
    Count_DQAccounts = 4
    Sum_PromiseAmount = 5
    Count_BarredAccounts = 6
    Count_HCAAccounts = 7
    Count_ADA = 8
    Count_VIP = 9
    Count_ActiveSubscriptions_CorpCustomer = 10
    Count_BarredSubscription_CorpCustomer = 11
    Count_SubscriptionForTermination_CorpCustomer = 12
    Count_TerminatedSubscriptions_CorpCustomer = 13
    Count_Active_Subscription_IndivCustomer = 14
    Count_BarredSubscription_IndivCustomer = 15
    Count_TerminatedSubscription_IndivCustomer = 16
    Account_DQ_Rate = 17
    Total_AR_DQ_Rate = 18
    Active_AR_DQ_Rate = 19
    Total_Balance_of_DQ_accounts = 20
    Bucket_Amount = 21

    def process(self):
        db = create_engine("postgresql+psycopg2://postgres:common@localhost:5432/postgres")
        #db = create_engine(f"postgresql+psycopg2://{ssm.username}{ssm.password}@{os.environ['HOSTNAME']}\
        # :{os.environ['POST']}/{os.environ['DBNAME']})"
    
        metadata = MetaData(db)
        connection = db.connect()
        Session = sessionmaker(db)
        session = Session()

        mapping = {
            "Total_Account_ForBarring": TotalAccount_ForBarring,
            "Total_Accounts_ForTermination": TotalAccount_ForTermination,
            "Count_ActiveAccounts": CountActiveAccounts,
            "Count_DQAccounts": CountDQAccounts,
            "Sum_PromiseAmount": Sum_PromiseAmount,
            "Count_BarredAccounts": Count_BarredAccounts,
            "Count_HCAAccounts": Count_HCAAccounts,
            "Count_ADA": Count_ADA,
            "Count_VIP": Count_VIP,
            "Count_ActiveSubscriptions_CorpCustomer": Count_ActvSubs_CorpCust,
            "Count_BarredSubscription_CorpCustomer":Count_BarredSubscription_CorpCustomer,
            "Count_SubscriptionForTermination_CorpCustomer":Count_SubscriptionForTermination_CorpCustomer,
            "Count_TerminatedSubscriptions_CorpCustomer":Count_TerminatedSubscriptions_CorpCustomer,
            "Count_Active_Subscription_IndivCustomer":Count_ActiveSubscription_IndivCustomer,
            "Count_BarredSubscription_IndivCustomer":Count_BarredSubscription_IndivCustomer,
            "Count_TerminatedSubscription_IndivCustomer":Count_TerminatedSubscription_IndivCustomer,
            "Account_DQ_Rate":Account_DQ_Rate,
            "Total_AR_DQ_Rate":Total_AR_DQ_Rate,
            "Active_AR_DQ_Rate":Active_AR_DQ_Rate,
            "Total_Balance_of_DQ_accounts":Total_Balance_of_DQ_accounts,
            "Bucket_Amount":Bucket_Amount
            }                
        mapping[self.name](session)


def main():
    
    field_id = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]

    for i in field_id:
        if i in CalculatedFields._value2member_map_:
            print(f"Start process {CalculatedFields(i).name}")
            CalculatedFields(i).process()
            print(f"End process {CalculatedFields(i).name}")
        else:
            print(f"Unsupported Calculated Field ID: {i}")


main()
