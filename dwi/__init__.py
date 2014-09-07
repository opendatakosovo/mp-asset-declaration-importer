import csv
from bson import ObjectId
from pymongo import MongoClient
from slugify import slugify

mongo = MongoClient()
mongo_collection = mongo.kosovoassembly.wealthdeclarations

# Clear collection before running importer
mongo_collection.remove({})

def import_data(csv_filepath, year):
	''' Imports deputy wealth declaration data from a CSV file into a MongoDB collection.
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

			deputy_name = row[0]
			if deputy_name != "":

				deputy_name_slug = slugify(deputy_name)

				party_acronym = row[1]
				party_name = row[1]
				party_slug = slugify(row[1])

				real_estate_individual = int(row[2]) if row[2] else 0
				real_estate_joint = int(row[3]) if row[3] else 0
	
				movable_individual = int(row[4]) if row[4] else 0
				movable_joint = int(row[5]) if row[5] else 0

				shares_individual = int(row[6]) if row[6] else 0
				shares_joint = int(row[7]) if row[7] else 0

				bonds_individual = int(row[8]) if row[8] else 0
				bonds_joint = int(row[9]) if row[9] else 0

				cash_individual = int(row[10]) if row[10] else 0
				cash_joint = int(row[11]) if row[11] else 0

				debts_or_outstanding_individual = int(row[12]) if row[12] else 0
				debts_or_outstanding_joint = int(row[13]) if row[13] else 0

				annual_salary_regular_individual = int(row[14]) if row[14] else 0
				annual_salary_regular_joint = int(row[15]) if row[15] else 0
	
				annual_salary_honorariums_individual = int(row[16]) if row[16] else 0
				annual_salary_honorariums_joint = int(row[17]) if row[17] else 0

				total_individual =	real_estate_individual + \
									movable_individual + \
									shares_individual + \
									bonds_individual + \
									cash_individual + \
									debts_or_outstanding_individual + \
									annual_salary_regular_individual + \
									annual_salary_honorariums_individual 

				total_joint = 	real_estate_joint + \
								movable_joint + \
								shares_joint + \
								bonds_joint + \
								cash_joint + \
								debts_or_outstanding_joint + \
								annual_salary_regular_joint + \
								annual_salary_honorariums_joint 


				total = total_individual + total_joint

				deputy_wealth_declaration = {
					"_id": str(ObjectId()),
					"year": year,
					"party": {
					 	"name": party_name,
						"acronym": party_acronym,
						"slug": party_slug
					},
					"deputy":{
						"name": deputy_name,
						"slug": deputy_name_slug
					},
					"realEstate":{
						"individual": real_estate_individual,
						"joint": real_estate_joint
					},
					"movable":{
						"individual": movable_individual,
						"joint": movable_joint
					},
					"shares":{
						"individual": shares_individual,
						"joint": shares_joint
					},
					"bonds":{
						"individual": bonds_individual,
						"joint": bonds_joint
					},
					"cash":{
						"individual": cash_individual,
						"joint": cash_joint
					},
					"debtsOrOutstanding":{
						"individual": debts_or_outstanding_individual,
						"joint": debts_or_outstanding_joint
					},
					"annualSalary":{
						"regular":{
							"individual": annual_salary_regular_individual,
							"joint": annual_salary_regular_joint
						},
						"honorariums":{
							"individual": annual_salary_honorariums_individual,
							"joint": annual_salary_honorariums_joint
						}
					},
					"totals":{
						"individual": total_individual,
						"joint": total_joint,
						"total": total
					}
				}

				print "%s (%s)" % (deputy_name, party_acronym)
				mongo_collection.insert(deputy_wealth_declaration)
				document_counter = document_counter + 1
			# End of if check. Check if read line is a declaration of wealth.
		# End for loop.
	# End file reading.


	return document_counter
