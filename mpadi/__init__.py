import csv
from bson import ObjectId
from pymongo import MongoClient
from slugify import slugify
from numpy import median
import pymongo

mongo = MongoClient()
mp_asset_declaration_collection = mongo.kosovoassembly.mpassetdeclarations
mp_asset_declaration_median_collection = mongo.kosovoassembly.mpassetdeclarationmedians

# Clear collections before running importer
mp_asset_declaration_collection.remove({})
mp_asset_declaration_median_collection.remove({})


def import_data(csv_filepath, year):
    ''' Imports deputy wealth declaration data.
    From a CSV file into a MongoDB collection.
    :param csv_filepath: the path of the CSV file.
    '''

    # This dictionary contains <key,value> objects that list all of the
    # PM assets in a Party.
    # The keys are the party names.
    # The values are objects containing lists of PM assets.
    # We will use it after we are done reading a CSV so that we can
    # caluclate the Median Asset for each party.
    mp_assets_by_party = {}

    # List the assets of a single MP in a Party
    party_mp_assets = {}

    with open(csv_filepath, 'rb') as csvfile:
        reader = csv.reader(csvfile)

        # Skip the header rows (3 of them)
        next(reader, None)
        next(reader, None)
        next(reader, None)

        document_counter = 0

        # Iterate through the rows, retrieve desired values.
        for row in reader:

            mp_name = row[0]

            if mp_name != "":

                mp_name_slug = slugify(mp_name)

                party_acronym = row[1]
                party_name = row[1]
                party_slug = slugify(row[1])

                # If this is the first time we process a MP declration for
                # this Party then create a new Party MP assets object.
                if party_slug not in mp_assets_by_party:
                    party_mp_assets = get_empty_party_mp_assets_object()

                    # It's a new Party MP assets object, so init it with
                    # year and party info.
                    party_mp_assets['year'] = year

                    party_mp_assets['party']['acronym'] = party_acronym
                    party_mp_assets['party']['name'] = party_name
                    party_mp_assets['party']['slug'] = party_slug

                else:
                    # We previously processed a member of the same party,
                    # so just fetch the party's MP assets object and add
                    # to it
                    party_mp_assets = mp_assets_by_party[party_slug]

                # REAL ESTATE ASSETS
                real_estate_individual = float(row[2]) if row[2] else 0
                (real_estate_individual > 0) and \
                    party_mp_assets['realEstate']['individual'] \
                    .append(real_estate_individual)
                '''
                What the heck just happened in that previous line of code?!?!

                If (real_estate_individual > 0) is false, then short-circuiting
                will kick in and the right-hand side won't be evaluated.

                if (real_estate_individuall > 0) is true, then the right-hand
                side will be evaluated and the element will be appended.

                So basically we aren't adding the asset declaration to the list
                of assets if the asset amount is equal to 0.
                '''

                real_estate_joint = float(row[3]) if row[3] else 0
                (real_estate_joint > 0) and \
                    party_mp_assets['realEstate']['joint'] \
                    .append(real_estate_joint)

                # TODO: NEED TO INCLUDE TOTALS IN party_mp_assets

                # MOVABLE ASSETS
                movable_individual = float(row[4]) if row[4] else 0
                (movable_individual > 0) and \
                    party_mp_assets['movable']['individual'] \
                    .append(movable_individual)

                movable_joint = float(row[5]) if row[5] else 0
                (movable_joint > 0) and \
                    party_mp_assets['movable']['joint'] \
                    .append(movable_joint)

                # SHARES ASSETS
                shares_individual = float(row[6]) if row[6] else 0
                (shares_individual > 0) and \
                    party_mp_assets['shares']['individual'] \
                    .append(shares_individual)

                shares_joint = float(row[7]) if row[7] else 0
                (shares_joint > 0) and \
                    party_mp_assets['shares']['joint'] \
                    .append(shares_joint)

                # BONDS ASSETS
                bonds_individual = float(row[8]) if row[8] else 0
                (bonds_individual > 0) and \
                    party_mp_assets['bonds']['individual'] \
                    .append(bonds_individual)

                bonds_joint = float(row[9]) if row[9] else 0
                (bonds_joint > 0) and \
                    party_mp_assets['bonds']['joint'] \
                    .append(bonds_joint)

                # CASH ASSETS
                cash_individual = float(row[10]) if row[10] else 0
                (cash_individual > 0) and \
                    party_mp_assets['cash']['individual'] \
                    .append(cash_individual)

                cash_joint = float(row[11]) if row[11] else 0
                (cash_joint > 0) and \
                    party_mp_assets['cash']['joint'] \
                    .append(cash_joint)

                # DEBTS OR OUTSTANDING ASSETS
                debts_or_outstanding_individual = float(row[12]) if row[12] else 0
                (debts_or_outstanding_individual > 0) and \
                    party_mp_assets['debtsOrOutstanding']['individual'] \
                    .append(debts_or_outstanding_individual)

                debts_or_outstanding_joint = float(row[13]) if row[13] else 0
                (debts_or_outstanding_joint > 0) and \
                    party_mp_assets['debtsOrOutstanding']['joint'] \
                    .append(debts_or_outstanding_joint)

                # ANNUAL SALARY ASSETS
                annual_salary_regular_individual = float(row[14]) if row[14] else 0
                (annual_salary_regular_individual > 0) and \
                    party_mp_assets['annualSalary']['regular']['individual'] \
                    .append(annual_salary_regular_individual)

                annual_salary_regular_joint = float(row[15]) if row[15] else 0
                (annual_salary_regular_joint > 0) and \
                    party_mp_assets['annualSalary']['regular']['joint'] \
                    .append(annual_salary_regular_joint)

                # ANNUAL SALARY HONORARIUMS
                annual_salary_honorariums_individual = float(row[16]) if row[16] else 0
                (annual_salary_honorariums_individual > 0) and \
                    party_mp_assets['annualSalary']['honorariums']['individual'] \
                    .append(annual_salary_honorariums_individual)

                annual_salary_honorariums_joint = float(row[17]) if row[17] else 0
                (annual_salary_honorariums_joint > 0) and \
                    party_mp_assets['annualSalary']['honorariums']['joint'] \
                    .append(annual_salary_honorariums_joint)

                # TOTAL INDIVIDUAL ASSETS
                total_individual =  \
                    real_estate_individual + \
                    movable_individual + \
                    shares_individual + \
                    bonds_individual + \
                    cash_individual - \
                    debts_or_outstanding_individual + \
                    annual_salary_regular_individual + \
                    annual_salary_honorariums_individual

                (total_individual > 0) and \
                    party_mp_assets['totals']['individual'] \
                    .append(total_individual)

                # TOTAL JOINT ASSETS
                total_joint = \
                    real_estate_joint + \
                    movable_joint + \
                    shares_joint + \
                    bonds_joint + \
                    cash_joint - \
                    debts_or_outstanding_joint + \
                    annual_salary_regular_joint + \
                    annual_salary_honorariums_joint

                (total_joint > 0) and \
                    party_mp_assets['totals']['joint'] \
                    .append(total_joint)

                # TOTAL ASSETS (Individual + Joint)
                total = total_individual + total_joint
                (total > 0) and party_mp_assets['totals']['total'].append(total)

                '''
                Now we build the asset declaration object for the current MP.
                '''
                mp_asset_declaration = {
                    "_id": str(ObjectId()),
                    "year": year,
                    "party": {
                        "name": party_name,
                        "acronym": party_acronym,
                        "slug": party_slug
                    },
                    "mp": {
                        "name": mp_name,
                        "slug": mp_name_slug
                    },
                    "realEstate": {
                        "individual": real_estate_individual,
                        "joint": real_estate_joint,
                        "total": real_estate_individual + real_estate_joint
                    },
                    "movable": {
                        "individual": movable_individual,
                        "joint": movable_joint,
                        "total": movable_individual + movable_joint
                    },
                    "shares": {
                        "individual": shares_individual,
                        "joint": shares_joint,
                        "total": shares_individual + shares_joint
                    },
                    "bonds": {
                        "individual": bonds_individual,
                        "joint": bonds_joint,
                        "total": bonds_individual + bonds_joint
                    },
                    "cash": {
                        "individual": cash_individual,
                        "joint": cash_joint,
                        "total": cash_individual + cash_joint
                    },
                    "debtsOrOutstanding": {
                        "individual": debts_or_outstanding_individual,
                        "joint": debts_or_outstanding_joint,
                        "total": debts_or_outstanding_individual + debts_or_outstanding_joint
                    },
                    "annualSalary": {
                        "regular": {
                            "individual": annual_salary_regular_individual,
                            "joint": annual_salary_regular_joint,
                            "total": annual_salary_regular_individual + annual_salary_regular_joint
                        },
                        "honorariums": {
                            "individual": annual_salary_honorariums_individual,
                            "joint": annual_salary_honorariums_joint,
                            "total": annual_salary_honorariums_individual + annual_salary_honorariums_joint
                        },
                        'totals': {
                            'individual': annual_salary_regular_individual + annual_salary_honorariums_individual,
                            'joint': annual_salary_regular_joint + annual_salary_honorariums_joint,
                            'total': annual_salary_regular_individual + annual_salary_honorariums_individual + annual_salary_regular_joint + annual_salary_honorariums_joint
                        }
                    },
                    "totals": {
                        "individual": total_individual,
                        "joint": total_joint,
                        "total": total
                    }
                }

                print "%s (%s)" % (mp_name, party_acronym)
                mp_asset_declaration_collection.insert(mp_asset_declaration)
                document_counter = document_counter + 1

                '''
                Save the Party MP assets object in the dictionary
                that contains <key,value> objects that list all of the
                PM assets in a Party.

                If the next asset declaration we process is from of an MP
                belong to the same Party then the new iteration will simply
                Retrieve this same Party MP assets object add append the
                assets lists.
                '''
                mp_assets_by_party[party_slug] = party_mp_assets

            # End of if check. Check if read line is a declaration of wealth.
        # End for loop.
    # End file reading.

    persist_asset_medians(year, mp_assets_by_party)

    return document_counter


def persist_asset_medians(year, mp_assets_by_party):
    ''' Build a document containing the median assets for each asset source.
    Persist that document in MongoDB.
    :param mp_assets_by_party: the dictionary listing all assets grouped by Party.
    '''

    print '\n\nMedian of total asset declarations (%i):\n' % year

    for party_slug, party_mp_assets in mp_assets_by_party.iteritems():

        party_assets_median = {
            "year": party_mp_assets['year'],
            "party": {
                "name": party_mp_assets['party']['name'],
                "acronym": party_mp_assets['party']['acronym'],
                "slug": party_mp_assets['party']['slug']
            },
            "realEstate": {
                "individual": median(party_mp_assets['realEstate']['individual']) if len(party_mp_assets['realEstate']['individual']) > 0 else 0,
                "joint": median(party_mp_assets['realEstate']['joint']) if len(party_mp_assets['realEstate']['joint']) > 0 else 0,
                "total": median(party_mp_assets['realEstate']['total']) if len(party_mp_assets['realEstate']['total']) > 0 else 0
            },
            "movable": {
                "individual": median(party_mp_assets['movable']['individual']) if len(party_mp_assets['movable']['individual']) > 0 else 0,
                "joint": median(party_mp_assets['movable']['joint']) if len(party_mp_assets['movable']['joint']) > 0 else 0,
                "total": median(party_mp_assets['movable']['total']) if len(party_mp_assets['movable']['total']) > 0 else 0
            },
            "shares": {
                "individual": median(party_mp_assets['shares']['individual']) if len(party_mp_assets['shares']['individual']) > 0 else 0,
                "joint": median(party_mp_assets['shares']['joint']) if len(party_mp_assets['shares']['joint']) > 0 else 0,
                "total": median(party_mp_assets['shares']['total']) if len(party_mp_assets['shares']['total']) > 0 else 0
            },
            "bonds": {
                "individual": median(party_mp_assets['bonds']['individual']) if len(party_mp_assets['bonds']['individual']) > 0 else 0,
                "joint": median(party_mp_assets['bonds']['joint']) if len(party_mp_assets['bonds']['joint']) > 0 else 0,
                "total": median(party_mp_assets['bonds']['total']) if len(party_mp_assets['bonds']['total']) > 0 else 0
            },
            "cash": {
                "individual": median(party_mp_assets['cash']['individual']) if len(party_mp_assets['cash']['individual']) > 0 else 0,
                "joint": median(party_mp_assets['cash']['joint']) if len(party_mp_assets['cash']['joint']) > 0 else 0,
                "total": median(party_mp_assets['cash']['total']) if len(party_mp_assets['cash']['total']) > 0 else 0
            },
            "debtsOrOutstanding": {
                "individual": median(party_mp_assets['debtsOrOutstanding']['individual']) if len(party_mp_assets['debtsOrOutstanding']['individual']) > 0 else 0,
                "joint": median(party_mp_assets['debtsOrOutstanding']['joint']) if len(party_mp_assets['debtsOrOutstanding']['joint']) > 0 else 0,
                "total": median(party_mp_assets['debtsOrOutstanding']['total']) if len(party_mp_assets['debtsOrOutstanding']['total']) > 0 else 0
            },
            "annualSalary": {
                "regular": {
                    "individual": median(party_mp_assets['annualSalary']['regular']['individual']) if len(party_mp_assets['annualSalary']['regular']['individual']) > 0 else 0,
                    "joint": median(party_mp_assets['annualSalary']['regular']['joint']) if len(party_mp_assets['annualSalary']['regular']['joint']) > 0 else 0,
                    "total": median(party_mp_assets['annualSalary']['regular']['total']) if len(party_mp_assets['annualSalary']['regular']['total']) > 0 else 0
                },
                "honorariums": {
                    "individual": median(party_mp_assets['annualSalary']['honorariums']['individual']) if len(party_mp_assets['annualSalary']['honorariums']['individual']) > 0 else 0,
                    "joint": median(party_mp_assets['annualSalary']['honorariums']['joint']) if len(party_mp_assets['annualSalary']['honorariums']['joint']) > 0 else 0,
                    "total": median(party_mp_assets['annualSalary']['honorariums']['total']) if len(party_mp_assets['annualSalary']['honorariums']['total']) > 0 else 0
                },
                'totals': {
                    "individual": median(party_mp_assets['annualSalary']['totals']['individual']) if len(party_mp_assets['annualSalary']['totals']['individual']) > 0 else 0,
                    "joint": median(party_mp_assets['annualSalary']['totals']['joint']) if len(party_mp_assets['annualSalary']['totals']['joint']) > 0 else 0,
                    "total": median(party_mp_assets['annualSalary']['totals']['total']) if len(party_mp_assets['annualSalary']['totals']['total']) > 0 else 0
                }
            },
            "totals": {
                "individual": median(party_mp_assets['totals']['individual']) if len(party_mp_assets['totals']['individual']) > 0 else 0,
                "joint": median(party_mp_assets['totals']['joint']) if len(party_mp_assets['totals']['joint']) > 0 else 0,
                "total": median(party_mp_assets['totals']['total']) if len(party_mp_assets['totals']['total']) > 0 else 0
            }
        }

        print '     %s: %s' % (party_mp_assets['party']['acronym'], party_assets_median['totals']['total'])
        mp_asset_declaration_median_collection.insert(party_assets_median)


def get_empty_party_mp_assets_object():
    '''Create a blank Party MP Assets Object.
    By blank we mean that the values are either empty strings, empty arrays or -1.
    '''

    empty_party_mp_assets = {
        "year": -1,
        "party": {
            "name": '',
            "acronym": '',
            "slug": ''
        },
        "realEstate": {
            "individual": [],
            "joint": [],
            "total": []
        },
        "movable": {
            "individual": [],
            "joint": [],
            "total": []
        },
        "shares": {
            "individual": [],
            "joint": [],
            "total": []
        },
        "bonds": {
            "individual": [],
            "joint": [],
            "total": []
        },
        "cash": {
            "individual": [],
            "joint": [],
            "total": []
        },
        "debtsOrOutstanding": {
            "individual": [],
            "joint": [],
            "total": []
        },
        "annualSalary": {
            "regular": {
                "individual": [],
                "joint": [],
                "total": []
            },
            "honorariums": {
                "individual": [],
                "joint": [],
                "total": []
            },
            'totals': {
                'individual': [],
                'joint': [],
                'total': []
            }
        },
        "totals": {
            "individual": [],
            "joint": [],
            "total": []
        }
    }

    return empty_party_mp_assets


def persist_medians_document(year, party, totals):
    print sorted(totals)
    medians = {
        'party': {
            'name': party['name'],
            'acronym': party['acronym'],
            'slug': party['slug']
        },
        'year': year,
        'medians': {
            'totals': {
                'individual': 0,
                'joint': 0,
                'total': median(totals)
            }
        }
    }

    print medians
    print ''
