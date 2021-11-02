from models import *
from sqlalchemy.sql import func
from traceback import print_exc

def Count_ActiveSubscription_IndivCustomer(session):
    """
    Count of subscriptions where PCC_Subscription.SUBSCRIPTION_STATUS = 'Active' for 
    Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
    All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table.
    """

    try:
        leg_data = (
            session.query(
                PCC_Subscription.Entity_ID,
                func.count(PCC_Subscription.subscription_status).label("Count"),
            )
            .filter(PCC_Subscription.subscription_status == "Active")
            .group_by(PCC_Subscription.Entity_ID, PCC_Subscription.subscription_status)
        )

        for i in leg_data:
            session.add(PCC_ENTITYAGG_DATA(Entity_ID=i.Entity_ID, CNT_ACTIVE_SUBSC_IC=i.Count))

    except Exception:
        print_exc()
    


    try:
        crp_data = (
            session.query(
                PCC_Subscription.Corp_ID,
                func.count(PCC_Subscription.subscription_status).label("Count"),
            )
            .filter(PCC_Subscription.subscription_status == "Active")
            .group_by(PCC_Subscription.Corp_ID, PCC_Subscription.subscription_status)
        )

        for i in crp_data:
            session.add(PCC_CORPAGG_DATA(Corp_ID=i.Corp_ID, CNT_ACTIVE_SUBSC_IC=i.Count))

    except Exception:
        print_exc()

    try:
        cst_data = (
            session.query(
                PCC_Subscription.Customer_ID,
                func.count(PCC_Subscription.subscription_status).label("Count"),
            )
            .filter(PCC_Subscription.subscription_status == "Active")
            .group_by(PCC_Subscription.Customer_ID, PCC_Subscription.subscription_status)
        )

        for i in cst_data:
            session.add(PCC_CUSTAGG_DATA(Customer_ID=i.Customer_ID, CNT_ACTIVE_SUBSC_IC=i.Count))

    except Exception:
        print_exc()

    session.commit()
