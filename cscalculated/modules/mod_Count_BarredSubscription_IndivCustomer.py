from models import *
from sqlalchemy.sql import func

def Count_BarredSubscription_IndivCustomer(session):
    """
    Count of subscriptions where PCC_Subscription.SUBSCRIPTION_STATUS = 'Barred'
    Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
    All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table."
    """

    leg_data = (
        session.query(
            PCC_Subscription.Entity_ID,
            func.count(PCC_Subscription.subscription_status).label("Count"),
        )
        .filter(PCC_Subscription.subscription_status == "Barred")
        .group_by(PCC_Subscription.Entity_ID, PCC_Subscription.subscription_status)
    )

    crp_data = (
        session.query(
            PCC_Subscription.Corp_ID,
            func.count(PCC_Subscription.subscription_status).label("Count"),
        )
        .filter(PCC_Subscription.subscription_status == "Barred")
        .group_by(PCC_Subscription.Corp_ID, PCC_Subscription.subscription_status)
    )

    cst_data = (
        session.query(
            PCC_Subscription.Customer_ID,
            func.count(PCC_Subscription.subscription_status).label("Count"),
        )
        .filter(PCC_Subscription.subscription_status == "Barred")
        .group_by(PCC_Subscription.Customer_ID, PCC_Subscription.subscription_status)
    )

    for i in leg_data:
        session.add(PCC_ENTITYAGG_DATA(Entity_ID=i.Entity_ID, CNT_BARREDSUBSC_IC=i.Count))

    for i in crp_data:
        session.add(PCC_CORPAGG_DATA(Corp_ID=i.Corp_ID, CNT_BARREDSUBSC_IC=i.Count))

    for i in cst_data:
        session.add(PCC_CUSTAGG_DATA(Customer_ID=i.Customer_ID, CNT_BARREDSUBSC_IC=i.Count))

    session.commit()
