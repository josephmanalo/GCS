from models import *
from sqlalchemy.sql import func, and_
from traceback import print_exc

def Active_AR_DQ_Rate(session):
    """
    Formula: (Sum of outstanding amount of delinquent Accounts/ sum of outstanding amount for all accounts) * 100
    'only for accounts where account_status = 'Active'

    Sum of outstanding amount of delinquent Accounts: Check logic for '4. Count_DQAccounts' 
    for identifying the delinquent accounts.
    Sum the outstanding from PCC_Account.OUTSTANDING_BALANCE.

    Sum of outstanding amount for all accounts from 'PCC_Account'  
    Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
    All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table.
    """

    
    Sum_Balance_of_Accounts = session.query(
        func.sum(PCC_Account.outstanding_balance).label('total_sum')
    ).where(PCC_Account.account_status == "Active").first()['total_sum']

  
    try:
        gbu_data = (
            session.query(
                PCC_Account.GBU_ID,
                func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
            )
            .where(
                and_(PCC_Account.delinquent_account == "Y", PCC_Account.account_status == "Active")
            )
            .group_by(PCC_Account.GBU_ID)
        )
        
        for i in gbu_data:
            GBU_AR_DQRate = (i.sum_oustanding_balance / Sum_Balance_of_Accounts) * 100
            session.add(PCC_GBUAGG_DATA(GBU_ID=i.GBU_ID, ACTIVE_AR_DQRATE=GBU_AR_DQRate))

    except Exception:
        print_exc()

    try:
        grp_data = (
            session.query(
                PCC_Account.Group_ID,
                func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
            )
            .where(
                and_(PCC_Account.delinquent_account == "Y", PCC_Account.account_status == "Active")
            )
            .group_by(PCC_Account.Group_ID)
        )

        for i in grp_data:
            Group_AR_DQRate = (i.sum_oustanding_balance / Sum_Balance_of_Accounts) * 100
            session.add(PCC_COMPGAGG_DATA(Group_ID=i.Group_ID, ACTIVE_AR_DQRATE=Group_AR_DQRate))

    except Exception:
        print_exc()


    try:
        leg_data = (
            session.query(
                PCC_Account.Entity_ID,
                func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
            )
            .where(
                and_(PCC_Account.delinquent_account == "Y", PCC_Account.account_status == "Active")
            )
            .group_by(PCC_Account.Entity_ID)
        )

        for i in leg_data:
            Entity_AR_DQRate = (i.sum_oustanding_balance / Sum_Balance_of_Accounts) * 100
            session.add(PCC_ENTITYAGG_DATA(Entity_ID=i.Entity_ID, ACTIVE_AR_DQRATE=Entity_AR_DQRate))

    except Exception:
        print_exc()
  
    try:
        crp_data = (
            session.query(
                PCC_Account.Corp_ID,
                func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
            )
            .where(
                and_(PCC_Account.delinquent_account == "Y", PCC_Account.account_status == "Active")
            )
            .group_by(PCC_Account.Corp_ID)
        )

        for i in crp_data:
            Corp_AR_DQRate = (i.sum_oustanding_balance / Sum_Balance_of_Accounts) * 100
            session.add(PCC_CORPAGG_DATA(Corp_ID=i.Corp_ID, ACTIVE_AR_DQRATE=Corp_AR_DQRate))
    except Exception:
        print_exc()
 


    try:
        cst_data = (
            session.query(
                PCC_Account.Customer_ID,
                func.sum(PCC_Account.outstanding_balance).label("sum_oustanding_balance"),
            )
            .where(
                and_(PCC_Account.delinquent_account == "Y", PCC_Account.account_status == "Active")
            )
            .group_by(PCC_Account.Customer_ID)
        )

        for i in cst_data:
            Cust_AR_DQRate = (i.sum_oustanding_balance / Sum_Balance_of_Accounts) * 100
            session.add(PCC_CUSTAGG_DATA(Customer_ID=i.Customer_ID, ACTIVE_AR_DQRATE=Cust_AR_DQRate))

    except Exception:
        print_exc()

    session.commit()