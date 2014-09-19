import csv
from bson import ObjectId
from pymongo import MongoClient
from slugify import slugify

mongo = MongoClient()
mp_asset_declaration_collection = mongo.kosovoassembly.mpassetdeclarations

# Clear collections before running importer
mp_asset_declaration_collection.remove({})


def import_data(csv_filepath, year):
    ''' Imports deputy wealth declaration data.
    From a CSV file into a MongoDB collection.
    :param csv_filepath: the path of the CSV file.
    '''

    with open(csv_filepath, 'rb') as csvfile:
        reader = csv.reader(csvfile)

        # Skip the header rows (3 of them)
        next(reader, None)
        next(reader, None)
        next(reader, None)

        document_counter = 0

        # Iterate through the rows, retrieve desired values.
        for row in reader:

            mp_name = row[0].strip()

            if mp_name != "":

                mp_name_slug = slugify(mp_name)

                # In the last mandate AKR were they were represented as KKR
                # Wherever in the List is KKR acronym, that is AKR,
                # merge those data together in each specific year.
                party_acronym = row[1].strip()
                if party_acronym == 'KKR':
                    party_acronym = 'AKR'

                party_name = party_acronym
                party_slug = slugify(party_acronym)

                # REAL ESTATE ASSETS
                real_estate_individual = float(row[2]) if row[2] else 0
                real_estate_joint = float(row[3]) if row[3] else 0
                real_estate_total = real_estate_individual + real_estate_joint

                # MOVABLE ASSETS
                movable_individual = float(row[4]) if row[4] else 0
                movable_joint = float(row[5]) if row[5] else 0
                movable_total = movable_individual + movable_joint

                # SHARES ASSETS
                shares_individual = float(row[6]) if row[6] else 0
                shares_joint = float(row[7]) if row[7] else 0
                shares_total = shares_individual + shares_joint

                # BONDS ASSETS
                bonds_individual = float(row[8]) if row[8] else 0
                bonds_joint = float(row[9]) if row[9] else 0
                bonds_total = bonds_individual + bonds_joint

                # CASH ASSETS
                cash_individual = float(row[10]) if row[10] else 0
                cash_joint = float(row[11]) if row[11] else 0
                cash_total = cash_individual + cash_joint

                # DEBTS OR OUTSTANDING ASSETS
                debts_or_outstanding_individual = float(row[12]) if row[12] else 0
                debts_or_outstanding_joint = float(row[13]) if row[13] else 0
                debts_or_outstanding_total = debts_or_outstanding_individual + debts_or_outstanding_joint

                # ANNUAL SALARY ASSETS
                annual_salary_regular_individual = float(row[14]) if row[14] else 0
                annual_salary_regular_joint = float(row[15]) if row[15] else 0
                annual_salary_regular_total = annual_salary_regular_individual + annual_salary_regular_joint

                # ANNUAL SALARY HONORARIUMS
                annual_salary_honorariums_individual = float(row[16]) if row[16] else 0
                annual_salary_honorariums_joint = float(row[17]) if row[17] else 0
                annual_salary_honorariums_total = annual_salary_honorariums_individual + annual_salary_honorariums_joint

                # ANNUAL SALARY TOTAL
                annual_salary_individual_total = annual_salary_regular_individual + annual_salary_honorariums_individual
                annual_salary_joint_total = annual_salary_regular_joint + annual_salary_honorariums_joint
                annual_salary_total = annual_salary_individual_total + annual_salary_joint_total


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

                # TOTAL ASSETS (Individual + Joint)
                total = total_individual + total_joint

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
                        "total": real_estate_total
                    },
                    "movable": {
                        "individual": movable_individual,
                        "joint": movable_joint,
                        "total": movable_total
                    },
                    "shares": {
                        "individual": shares_individual,
                        "joint": shares_joint,
                        "total": shares_total
                    },
                    "bonds": {
                        "individual": bonds_individual,
                        "joint": bonds_joint,
                        "total": bonds_total
                    },
                    "cash": {
                        "individual": cash_individual,
                        "joint": cash_joint,
                        "total": cash_total
                    },
                    "debtsOrOutstanding": {
                        "individual": debts_or_outstanding_individual,
                        "joint": debts_or_outstanding_joint,
                        "total": debts_or_outstanding_total
                    },
                    "annualSalary": {
                        "regular": {
                            "individual": annual_salary_regular_individual,
                            "joint": annual_salary_regular_joint,
                            "total": annual_salary_regular_total
                        },
                        "honorariums": {
                            "individual": annual_salary_honorariums_individual,
                            "joint": annual_salary_honorariums_joint,
                            "total": annual_salary_honorariums_total
                        },
                        'totals': {
                            'individual': annual_salary_individual_total,
                            'joint': annual_salary_joint_total,
                            'total': annual_salary_total
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

            # End of if check. Check if read line is a declaration of wealth.
        # End for loop.
    # End file reading.

    return document_counter
