#!/usr/bin/python3

import logging
import pprint
import yaml
import json
from pathlib import Path
import gridfs
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from simtools import db_handler


def readDescriptions(prefixInputDB):

    sections = [
        'Telescope_optics',
        'Camera',
        'Photon_conversion',
        'Trigger',
        'Readout_electronics',
        'Sites_DB'
        ]

    descriptions = dict()

    for sectionNow in sections:
        inputFileNow = '{}/descriptionsYml/{}.yml'.format(prefixInputDB, sectionNow)
        with open(inputFileNow, 'r') as stream:
            descriptions.update(yaml.load(stream, Loader=yaml.FullLoader))

    # Read also raw sim_telarray descriptions of parameters which are not in the reports.
    inputFileNow = '{}/descriptionsYml/otherDescriptions.yml'.format(prefixInputDB)
    with open(inputFileNow, 'r') as stream:
        otherDescriptions = yaml.load(stream, Loader=yaml.FullLoader)

    for parNow, descNow in otherDescriptions.items():
        if parNow in descriptions:
            continue
        else:
            descriptions[parNow] = descNow

    return descriptions


def readOneTelYamlDB(fInputDB):

    with open(fInputDB, 'r') as stream:
        parsDB = yaml.load(stream, Loader=yaml.FullLoader)

    return parsDB


def insertFileToDB(db, file, **kwargs):

    fileSystem = gridfs.GridFS(db)
    if fileSystem.exists({'filename': kwargs['filename']}):
        return fileSystem.find_one({'filename': kwargs['filename']})

    with open(file, 'rb') as dataFile:
        file_id = fileSystem.put(dataFile, **kwargs)

    return file_id


def insertFilesToDB(dbClient, dbName, filesToAddToDB):

    db = dbClient[dbName]
    for fileNow in filesToAddToDB:
        kwargs = {'content_type': 'ascii/dat', 'filename': Path(fileNow).name}
        insertFileToDB(db, fileNow, **kwargs)

    return


def dropDatabases(dbClient):

    dbClient.drop_database(DB_TABULATED_DATA)
    dbClient.drop_database(DB_CTA_SIMULATION_MODEL)
    dbClient.drop_database(DB_CTA_SIMULATION_MODEL_DESCRIPTIONS)

    return


def getFile(dbClient, dbName, fileName):

    db = dbClient[dbName]
    fileSystem = gridfs.GridFS(db)
    if fileSystem.exists({'filename': fileName}):
        return fileSystem.find_one({'filename': fileName})
    else:
        raise FileNotFoundError(
            'The file {} does not exist in the database {}'.format(fileName, dbName)
        )


def writeFileToDisk(dbClient, dbName, path, file):

    db = dbClient[dbName]
    fsOutput = gridfs.GridFSBucket(db)
    with open(Path(path).joinpath(file.filename), 'wb') as outputFile:
        fsOutput.download_to_stream_by_name(file.filename, outputFile)

    return


def getYamlDB(telescopeType, prefixInputDB):

    telNameInYamlDB = telescopeType
    if telNameInYamlDB == 'MST-Structure':
        telNameInYamlDB = 'MST-optics'
    if telNameInYamlDB in sstNamesDict.keys():
        telNameInYamlDB = sstNamesDict[telescopeType]
    fileInputDB = '{}configReports/parValues-{}.yml'.format(prefixInputDB, telNameInYamlDB)

    return readOneTelYamlDB(fileInputDB)


def inferType(descriptions, par, value):

    if par not in descriptions.keys():
        return str

    parDesc = descriptions[par]

    if 'type' not in parDesc:
        return str

    typeNow = parDesc['type'].lower()
    if typeNow in ['string', 'text', 'unknown']:
        return str
    elif typeNow == 'double':
        try:
            _value = float(value)
            return float
        except ValueError:
            return str
    elif 'int' in typeNow:  # Include also UInt
        try:
            _value = int(value)
            return int
        except ValueError:
            return str
    else:
        return str  # Shouldn't actually reach here


def additionalEntries(parDesc):

    addEntriesDict = dict()

    if 'unit' in parDesc:
        addEntriesDict['unit'] = parDesc['unit']
    if 'items' in parDesc:
        addEntriesDict['items'] = parDesc['items']
    if 'minimum' in parDesc:
        addEntriesDict['minimum'] = parDesc['minimum']
    if 'maximum' in parDesc:
        addEntriesDict['maximum'] = parDesc['maximum']

    return addEntriesDict


def getDescriptions(parDesc):

    descriptionDict = dict()

    if 'description' in parDesc:
        descriptionDict['description'] = parDesc['description']
    if 'shortDescription' in parDesc:
        descriptionDict['shortDescription'] = parDesc['shortDescription']

    if 'assembly' in parDesc:
        descriptionDict['assembly'] = parDesc['assembly']
    if 'parOrAlg' in parDesc:
        descriptionDict['parOrAlg'] = parDesc['parOrAlg']

    # These are for skipping printing parameter based on list condition,
    # i.e., print only if the value of parameter A is in the list 1,2,3 ---> { A : [1,2,3] }
    if 'printIf' in parDesc:
        descriptionDict['printIf'] = parDesc['printIf']
    if 'printIfNot' in parDesc:
        descriptionDict['printIfNot'] = parDesc['printIfNot']

    # These are for skipping printing parameter based on their value (e.g., zero)
    if 'printIfValue' in parDesc:
        descriptionDict['printIfValue'] = parDesc['printIfValue']
    if 'printIfNotValue' in parDesc:
        descriptionDict['printIfNotValue'] = parDesc['printIfNotValue']

    # These are for skipping printing parameter based on the value of another parameter
    if 'printIfValueEqualTo' in parDesc:
        descriptionDict['printIfValueEqualTo'] = parDesc['printIfValueEqualTo']
    if 'printIfValueNotEqualTo' in parDesc:
        descriptionDict['printIfValueNotEqualTo'] = parDesc['printIfValueNotEqualTo']

    return descriptionDict


def createDB(dbClient, prefixInputDB):

    dropDatabases(dbClient)

    descriptions = readDescriptions(prefixInputDB)

    filesToAddToDB = set()
    dbEntries = list()
    descriptionEntries = list()

    for site, layout in layouts.items():
        for telNow, telTypes in layout.items():

            if not isinstance(telTypes, list):
                telTypes = [telTypes]

            print('Preparing input for {} {} telescopes in the {}'.format(
                len(telTypes),
                telNow,
                site)
            )

            pars = getYamlDB(telNow, prefixInputDB)

            # Extract a list of all versions currently in the yaml DB.
            # Can be extracted from a random telescope/parameter, as they are all the same.
            versions = pars['fadc_amplitude'].copy()
            versions.pop('Applicable', None)

            for telTypeNow in telTypes:

                for versionNow in versions:
                    for parNow in pars:

                        if versionNow not in pars[parNow]:
                            continue

                        dbEntry = dict()
                        dbEntry['Telescope'] = '{}-{}-{}'.format(site, telNow, telTypeNow)
                        dbEntry['Parameter'] = parNow
                        dbEntry['Applicable'] = pars[parNow]['Applicable']
                        dbEntry['Version'] = versionNow
                        value = pars[parNow][versionNow]  # Shorter for the code below
                        valueType = inferType(descriptions, parNow, value)
                        dbEntry['Type'] = str(valueType)

                        if '.dat' in str(value) or '.txt' in str(value):
                            dbEntry['File'] = True
                            if versionNow != 'default':
                                filesToAddToDB.add('{}datFiles/{}'.format(prefixInputDB, value))
                        else:
                            dbEntry['File'] = False

                        try:
                            value = valueType(value)
                        except ValueError:
                            pass

                        dbEntry['Value'] = value

                        dbEntry.update(additionalEntries(descriptions[parNow]))
                        dbEntries.append(dbEntry)

    print('Preparing description entries')
    for parNow in pars:
        descNow = dict()
        descNow['Parameter'] = parNow
        descNow.update(getDescriptions(descriptions[parNow]))
        descriptionEntries.append(descNow)

    print('Filling {} DB'.format(DB_CTA_SIMULATION_MODEL))
    db = dbClient[DB_CTA_SIMULATION_MODEL]
    collection = db.telescopes
    try:
        collection.insert_many(dbEntries)
    except BulkWriteError as exc:
        raise exc(exc.details)

    print('Adding files to {} DB'.format(DB_TABULATED_DATA))
    insertFilesToDB(dbClient, DB_TABULATED_DATA, filesToAddToDB)

    print('Filling {} DB'.format(DB_CTA_SIMULATION_MODEL_DESCRIPTIONS))
    db = dbClient[DB_CTA_SIMULATION_MODEL_DESCRIPTIONS]
    collection = db.telescopes
    try:
        collection.insert_many(descriptionEntries)
    except BulkWriteError as exc:
        raise exc(exc.details)

    createSitesDB(dbClient, prefixInputDB, descriptions)

    addMetadata(dbClient)

    return


def writeJSON(dbClient, version='prod4', onlyApplicable=False):

    telsToRead = {
        'LST': {
            'dbName': 'North-LST-D234',
            'jsonName': 'Lx',
            'nTel': 4
        },
        'MST': {
            'dbName': ['North-MST-Structure-D', 'North-MST-NectarCam-D'],
            'jsonName': 'Mx',
            'nTel': 15
        }
    }

    with open('/Users/ogueta/work/cta/gammasim/gammasim-tools/play/db/sections.yml', 'r') as stream:
        try:
            sections = yaml.load(stream, Loader=yaml.FullLoader)
        except yaml.YAMLError as exc:
            raise exc

    collection = dbClient[DB_CTA_SIMULATION_MODEL].telescopes
    parameters = dict()
    for telNow, telNamesDict in telsToRead.items():

        genericTelPars = dict()
        telNameDB = telNamesDict['dbName']
        if not isinstance(telNameDB, list):
            telNameDB = [telNameDB]

        _parsFromDB = dict()
        for telNameNow in telNameDB:

            # We take the defaults from the structure entries
            if telNameNow == 'North-MST-FlashCam-D':
                onlyApplicable = True

            query = {
                'Telescope': telNameNow,
                'Version': version,
            }
            if onlyApplicable:
                query['Applicable'] = onlyApplicable

            for i_entry, post in enumerate(collection.find(query)):
                parNow = post['Parameter']
                _parsFromDB[parNow] = post
                _parsFromDB[parNow].pop('_id', None)
                _parsFromDB[parNow].pop('Parameter', None)
                _parsFromDB[parNow].pop('Telescope', None)

            for sectionNow, parList in sections.items():
                if sectionNow in ['Sites', 'Sections', 'Unnecessary']:
                    continue

                genericTelPars[sectionNow] = dict()
                genericTelPars[sectionNow]['id'] = sectionNow
                genericTelPars[sectionNow]['title'] = sectionNow
                genericTelPars[sectionNow]['val'] = 0
                genericTelPars[sectionNow]['children'] = list()
                for parNow in parList:

                    # if onlyApplicable:
                    #     if not _parsFromDB[parNow]['Applicable']:
                    #         continue
                    if parNow not in _parsFromDB:
                        continue

                    childDict = dict()
                    childDict['id'] = parNow
                    childDict['title'] = parNow
                    childDict['val'] = _parsFromDB[parNow]['Value']
                    genericTelPars[sectionNow]['children'].append(childDict)

        for i_tel in range(1, telNamesDict['nTel'] + 1):
            telNameJson = '{0}{1:02}'.format(telNamesDict['jsonName'], i_tel)
            parameters[telNameJson] = genericTelPars

    with open('telescopeModel.json', 'w') as fp:
        json.dump(parameters, fp, sort_keys=False, indent=4)


def updateParameter(dbClient, telescope, version, parameter, newValue):

    collection = dbClient[DB_CTA_SIMULATION_MODEL].telescopes

    query = {
        'Telescope': telescope,
        'Version': version,
        'Parameter': parameter,
    }

    parEntry = collection.find_one(query)
    oldValue = parEntry['Value']

    print('For telescope {}, version {}\nreplacing {} value from {} to {}'.format(
        telescope,
        version,
        parameter,
        oldValue,
        newValue
    ))

    queryUpdate = {'$set': {'Value': newValue}}

    collection.update_one(query, queryUpdate)

    return


def createSitesDB(dbClient, prefixInputDB, descriptions):

    print('Preparing sites DB')

    filesToAddToDB = set()
    dbEntries = list()
    descriptionEntries = list()
    sites = {
        'lapalma': 'North',
        'paranal': 'South'
    }

    pars = getYamlDB('Sites', prefixInputDB)

    # Extract a list of all versions currently in the yaml DB.
    # Can be extracted from a random parameter, as they are all the same.
    versions = pars['paranal_altitude'].copy()
    versions.pop('Applicable', None)

    for versionNow in versions:
        for parNow in pars:

            if versionNow not in pars[parNow]:
                continue

            dbEntry = dict()
            dbEntry['Site'] = sites[parNow.split('_')[0]]
            dbEntry['Parameter'] = '_'.join(parNow.split('_')[1:])
            dbEntry['Applicable'] = pars[parNow]['Applicable']
            dbEntry['Version'] = versionNow
            value = pars[parNow][versionNow]  # Shorter for the code below
            valueType = inferType(descriptions, parNow, value)
            dbEntry['Type'] = str(valueType)

            if any(ext in str(value) for ext in ['.dat', '.txt', '.lis']):
                dbEntry['File'] = True
                if versionNow != 'default':
                    filesToAddToDB.add('{}datFiles/{}'.format(prefixInputDB, value))
            else:
                dbEntry['File'] = False

            try:
                value = valueType(value)
            except ValueError:
                pass

            dbEntry['Value'] = value

            dbEntry.update(additionalEntries(descriptions[parNow]))
            dbEntries.append(dbEntry)

    print('Preparing description entries')
    for parNow in pars:
        descNow = dict()
        descNow['Parameter'] = '_'.join(parNow.split('_')[1:])
        descNow.update(getDescriptions(descriptions[parNow]))
        descriptionEntries.append(descNow)

    print('Filling {} DB'.format(DB_CTA_SIMULATION_MODEL))
    db = dbClient[DB_CTA_SIMULATION_MODEL]
    siteCollection = db.sites
    try:
        siteCollection.insert_many(dbEntries)
    except BulkWriteError as exc:
        raise exc(exc.details)

    print('Adding files to {} DB'.format(DB_TABULATED_DATA))
    insertFilesToDB(dbClient, DB_TABULATED_DATA, filesToAddToDB)

    print('Filling {} DB'.format(DB_CTA_SIMULATION_MODEL_DESCRIPTIONS))
    db = dbClient[DB_CTA_SIMULATION_MODEL_DESCRIPTIONS]
    siteCollection = db.sites
    try:
        siteCollection.insert_many(descriptionEntries)
    except BulkWriteError as exc:
        raise exc(exc.details)

    return


def addMetadata(dbClient):

    print('Adding Metadata')

    dbEntries = list()

    dbEntry = dict()
    dbEntry['Entry'] = 'Simulation-Model-Tags'
    dbEntry['Tags'] = {
        'Current': {
            'Value': '2020-06-28',
            'Label': 'Prod5'
        },
        'Latest': {
            'Value': '2020-06-28',
            'Label': 'Prod5'
        }
    }
    dbEntries.append(dbEntry)

    print('Filling {} DB'.format(DB_CTA_SIMULATION_MODEL))
    db = dbClient[DB_CTA_SIMULATION_MODEL]
    metadataCollection = db.metadata
    try:
        metadataCollection.insert_many(dbEntries)
    except BulkWriteError as exc:
        raise exc(exc.details)

    return


if __name__ == '__main__':

    logger = logging.getLogger('createDB')
    logger.setLevel('INFO')

    # dict in Python >=3.6 is ordered (finally),
    # so we can use this order to deal with MST structure/cameras reading sequence.
    layouts = {
        'North': {
            'LST': [1, 'D234'],
            'MST-Structure': 'D',
            'MST-FlashCam': 'D',
            'MST-NectarCam': 'D'
        },
        'South': {
            'LST': 'D',
            'MST-Structure': 'D',
            'MST-FlashCam': 'D',
            'SCT': 'D',
            'SST-Structure': 'D',
            'SST-Camera': 'D',
            'SST-ASTRI': 'D',
            'SST-1M': 'D',
            'SST-GCT': 'D'
        }
    }
    sstNamesDict = {
        'SST-ASTRI': 'SST-2M-ASTRI',
        'SST-1M': 'SST-1M',
        'SST-GCT': 'SST-2M-GCT-S'
    }

    prefixInputDB = (
        '/Users/ogueta/work/cta/aswg/simulations/simulation-model/simulation-model-description/'
    )

    DB_TABULATED_DATA = 'CTA-Simulation-Model'
    DB_CTA_SIMULATION_MODEL = 'CTA-Simulation-Model'
    DB_CTA_SIMULATION_MODEL_DESCRIPTIONS = 'CTA-Simulation-Model-Descriptions'
    # DB_TABULATED_DATA = 'cta'
    # DB_CTA_SIMULATION_MODEL = 'cta'

    remoteDB = True

    if remoteDB:
        db = db_handler.DatabaseHandler(logger.name)
        dbClient, _tunnel = db._openMongoDB()
    else:
        dbClient = MongoClient()

    pprint.pprint(dbClient.list_database_names())
    # dropDatabases(dbClient)

    createDB(dbClient, prefixInputDB)
    db.updateParameter(
        DB_CTA_SIMULATION_MODEL,
        'North-LST-1',
        '2020-06-28',
        'mirror_list',
        'mirror_CTA-N-LST1_v2019-03-31.dat'
    )
    db.updateParameter(
        DB_CTA_SIMULATION_MODEL,
        'North-LST-D234',
        '2020-06-28',
        'mirror_list',
        'mirror_CTA-N-LST2_v2020-04-07.dat'
    )
    # writeJSON(dbClient, 'prod4', False)

    # updateParameter(
    #     dbClient,
    #     'North-LST-1',
    #     '2020-06-28',
    #     'mirror_list',
    #     'mirror_CTA-N-LST1_v2019-03-31.dat'
    # )
    # updateParameter(
    #     dbClient,
    #     'North-LST-D234',
    #     '2020-06-28',
    #     'mirror_list',
    #     'mirror_CTA-N-LST2_v2020-04-07.dat'
    # )
