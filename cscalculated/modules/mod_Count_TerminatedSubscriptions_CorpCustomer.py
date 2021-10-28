from models import *
from sqlalchemy.sql import func

def Count_TerminatedSubscriptions_CorpCustomer(session):
    """
    Count of subscriptions where PCC_Subscription.SUBSCRIPTION_STATUS = 'Terminated'
    Grouped at each level GBU, Custom Group, Legal Entity, Corp ID, Customer ID.
    All these parameters GBU ID, Group ID, Entity ID, Corp ID and Customer ID is also available in PCC_Account table."
    """

    gbu_data = (
        session.query(
            PCC_Subscription.GBU_ID,
            func.count(PCC_Subscription.subscription_status).label("Count"),
        )
        .filter(PCC_Subscription.subscription_status == "Terminated")
        .group_by(PCC_Subscription.GBU_ID, PCC_Subscription.subscription_status)
    )

    grp_data = (
        session.query(
            PCC_Subscription.Group_ID,
            func.count(PCC_Subscription.subscription_status).label("Count"),
        )
        .filter(PCC_Subscription.subscription_status == "Terminated")
        .group_by(PCC_Subscription.Group_ID, PCC_Subscription.subscription_status)
    )

    leg_data = (
        session.query(
            PCC_Subscription.Entity_ID,
            func.count(PCC_Subscription.subscription_status).label("Count"),
        )
        .filter(PCC_Subscription.subscription_status == "Terminated")
        .group_by(PCC_Subscription.Entity_ID, PCC_Subscription.subscription_status)
    )

    crp_data = (
        session.query(
            PCC_Subscription.Corp_ID,
            func.count(PCC_Subscription.subscription_status).label("Count"),
        )
        .filter(PCC_Subscription.subscription_status == "Terminated")
        .group_by(PCC_Subscription.Corp_ID, PCC_Subscription.subscription_status)
    )

    cst_data = (
        session.query(
            PCC_Subscription.Customer_ID,
            func.count(PCC_Subscription.subscription_status).label("Count"),
        )
        .filter(PCC_Subscription.subscription_status == "Terminated")
        .group_by(PCC_Subscription.Customer_ID, PCC_Subscription.subscription_status)
    )

    for i in gbu_data:
        session.add(PCC_GBUAGG_DATA(GBU_ID=i.GBU_ID, CNT_TERMSUBSC_CC=i.Count))

    for i in grp_data:
        session.add(PCC_COMPGAGG_DATA(Group_ID=i.Group_ID, CNT_TERMSUBSC_CC=i.Count))

    for i in leg_data:
        session.add(PCC_ENTITYAGG_DATA(Entity_ID=i.Entity_ID, CNT_TERMSUBSC_CC=i.Count))

    for i in crp_data:
        session.add(PCC_CORPAGG_DATA(Corp_ID=i.Corp_ID, CNT_TERMSUBSC_CC=i.Count))

    for i in cst_data:
        session.add(PCC_CUSTAGG_DATA(Customer_ID=i.Customer_ID, CNT_TERMSUBSC_CC=i.Count))

    session.commit()
