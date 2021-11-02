from models import *
from sqlalchemy.sql import func
from traceback import print_exc

def Account_DQ_Rate(session):
    """
    Count of account from 'PCC_Account' where delinquent_account = 'Y'. 
    Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
    All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table.

    Formula: (Number of delinquent Accounts/ total number of accounts) * 100
    Number of delinquent accounts: Check logic for '4. Count_DQAccounts'.
    Total number of accounts: Count of account from 'PCC_Account'  
    Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
    All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table.

    """

    Total_No_of_Accounts = session.query(PCC_Account).count()

    try:
        gbu_data = (
            session.query(
                PCC_Account.GBU_ID, func.count(PCC_Account.delinquent_account).label("Count")
            )
            .filter(PCC_Account.delinquent_account == "Y")
            .group_by(PCC_Account.GBU_ID, PCC_Account.delinquent_account)
        )

        for i in gbu_data:
            GBU_DQ_Rate = (i.Count / Total_No_of_Accounts) * 100
            session.add(PCC_GBUAGG_DATA(GBU_ID=i.GBU_ID, ACCT_DQRATE=GBU_DQ_Rate))

    except Exception:
        print_exc()

    try:
        grp_data = (
            session.query(
                PCC_Account.Group_ID, func.count(PCC_Account.delinquent_account).label("Count")
            )
            .filter(PCC_Account.delinquent_account == "Y")
            .group_by(PCC_Account.Group_ID, PCC_Account.delinquent_account)
        )

        for i in grp_data:
            Group_DQ_Rate = (i.Count / Total_No_of_Accounts) * 100
            session.add(PCC_COMPGAGG_DATA(Group_ID=i.Group_ID, ACCT_DQRATE=Group_DQ_Rate))
     
    except Exception:
        print_exc()   

    try:
        leg_data = (
            session.query(
                PCC_Account.Entity_ID, func.count(PCC_Account.delinquent_account).label("Count")
            )
            .filter(PCC_Account.delinquent_account == "Y")
            .group_by(PCC_Account.Entity_ID, PCC_Account.delinquent_account)
        )
        for i in leg_data:
            Entity_DQ_Rate = (i.Count / Total_No_of_Accounts) * 100
            session.add(PCC_ENTITYAGG_DATA(Entity_ID=i.Entity_ID, ACCT_DQRATE=Entity_DQ_Rate))

    except Exception:
        print_exc()

    try:
        crp_data = (
            session.query(
                PCC_Account.Corp_ID, func.count(PCC_Account.delinquent_account).label("Count")
            )
            .filter(PCC_Account.delinquent_account == "Y")
            .group_by(PCC_Account.Corp_ID, PCC_Account.delinquent_account)
        )

        for i in crp_data:
            Corp_DQ_Rate = (i.Count / Total_No_of_Accounts) * 100
            session.add(PCC_CORPAGG_DATA(Corp_ID=i.Corp_ID, ACCT_DQRATE=Corp_DQ_Rate))

    except Exception:
        print_exc()
 
    try:
        cst_data = (
            session.query(
                PCC_Account.Customer_ID, func.count(PCC_Account.delinquent_account).label("Count")
            )
            .filter(PCC_Account.delinquent_account == "Y")
            .group_by(PCC_Account.Customer_ID, PCC_Account.delinquent_account)
        )

        for i in cst_data:
                Cust_DQ_Rate = (i.Count / Total_No_of_Accounts) * 100
                session.add(PCC_CUSTAGG_DATA(Customer_ID=i.Customer_ID, ACCT_DQRATE=Cust_DQ_Rate))
   
    except Exception:
        print_exc()
    
    session.commit()