import re

DESCRIPTION_TAGGING_RULES = {
    "Travel/Hotel": re.compile(r"(?:MARIOTT)|(?:HYATT)"),
    "Travel/Ridesharing": re.compile(r"(?:UBER)|(?:LYFT)"),
    "Travel/Flights": re.compile(r"(?:ALASKA AIR)|(?:AIRLINE)"),
    "Travel/Parking": re.compile(r"(?:DOUG FOX PARKING)"),
    "Travel/Car Rental": re.compile(r"(?:AVIS)"),
    "Food & Drink/Restaurants & Fast Food": re.compile(r"(?:MCDONALDS)|(?:TACOBELL)|(?:PANDA EXPRESS)|(?:PIZZA)|(?:CAFE)|(?:RESTAURANT)|(?:STARBUCKS)|(?:POPEYES)"),
    "Food & Drink/Grocery": re.compile(r"(?:SAFEWAY)|(?:COSTCO)|(?:KROGER)|(?:WINCO FOODS)|(?:FRED MEYER)"),
    "Food & Drink/Delivery": re.compile(r"(?:DOORDASH)"),
    "Food & Drink/Convinience Store": re.compile(r"(?:Amazon Go)"),
    "Shopping": re.compile(r"(?:TARGET)|(?:BEST BUY)|(?:AMAZON)|(?:SEPHORA)|(?:THE HOME DEPOT)|(?:THE CONTAINER STOR)"),
    "Utilities": re.compile(r"(?:COMCAST)|(?:VERIZON)|(?:WASHINGTON WATER)"),
    "Health & Fitness": re.compile(r"(?:REGENCE BLUESHIELD)|(?:ANYTIME FITNESS)|(?:LABCORP)|(?:CVS)|(?:HEADWAY)|(?:FITNESS CENTER)"),
    "Entertainment": re.compile(r"(?:NETFLIX)|(?:BOARDGAMEARENA)|(?:MOVIE)|(?:THEATER)|(?:CONCERT)|(?:STEAM)|(?:YOUTUBE)|(?:Audible)"),
    "Digital Services": re.compile(r"(?:VULTR)|(?:VPN)|(?:GOOGLE)"),
    "Home/Rent": re.compile(r"(?:Zelle To LESMAN CONTRERAS)"),
    "Savings": re.compile(r"(?:SAVE-UP TRANSFER)"),
    "Subscription": re.compile(r"(?:PATREON)|(?:NETFLIX)"),
    "Paypal": re.compile(r"(?:PAYPAL)"),
    "Savings": re.compile(r"(?:Save-Up Transfer)"),
    "Debt Payment/Credit Card": re.compile(r"(?:DISCOVER DC PYMNTS)|(?:APPLECARD)|(?:AMEX EPAYMENT)"),
    "Debt Payment/Car Loan": re.compile(r"(?:HONDA PMT)")
}

def tag_description(description: str):
    """
    Applies DESCRIPTION_TAGGING_RULES to the description. Returns an iterable of all applicable tags, if any.
    """
    return tuple(tag for tag, rule in DESCRIPTION_TAGGING_RULES.items() if rule.search(description))