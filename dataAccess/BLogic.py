from dataAccess import companySQL, videoSQL,jsonSQL,MLModelSQL,userSQL
from datetime import date, datetime


def create_Tables():
    company = companySQL.CompanySQL()
    company.createTable()

    user = userSQL.UserSQL()
    user.createTable()

    ml_model = MLModelSQL.MLModelSQL()
    ml_model.createTable()

    video = videoSQL.VideoSQL()
    video.createTable()

    json = jsonSQL.JsonSQL()
    json.createTable()


def addCompnay(company_name):
    obj = companySQL.CompanySQL()
    obj.insert(company_name)

def addUser(user_name,password):
    obj = userSQL.UserSQL()
    obj.insert(user_name,password)

def checkpassword(user_name, password):
    obj = userSQL.UserSQL()
    res = obj.checkUserPass(user_name,password)
    return res

def videoImport(title, description, filePath, thumbnail, category, company_id, user_id,filename_2_store):
    obj = videoSQL.VideoSQL()
    obj.insertFile(title, description, filePath, thumbnail, category, company_id, user_id)
    id = obj.retreiveFileByPath(filePath)
    filename = filename_2_store + '_' + str(company_id)
    obj.storeFileLocally(id, filename)


def jsonImport(filePath,video_ID , model_ID, condition,company_id,user_id, filename_2_store):
    obj = jsonSQL.JsonSQL()
    obj.insertFile(filePath,video_ID , model_ID, condition,company_id,user_id)
    id = obj.retreiveFileByPath(filePath)
    date_time =datetime.now().strftime('%Y-%m-%d--%H-%M-%S')
    filename = filename_2_store + '_' + date_time
    obj.storeFileLocally(id, filename)

def getLatestJsonFile(video_path):
    obj_video = videoSQL.VideoSQL()
    id = obj_video.retreiveFileByPath(video_path)
    obj_json = jsonSQL.JsonSQL()
    res = obj_json.retreiveJsonFilesVideoID(1,id) # Retrieves the last Json file related to this video
    if len(res) > 0 and res[0] != None:
        return res[0][1]
    else:
        print('No Json for this video path')
        return None

def mlModelImport(file_path, model_description,filename_2_store):
    obj = MLModelSQL.MLModelSQL()
    obj.insertFile(file_path,model_description)
    id = obj.retreiveFileByPath(file_path)
    obj.storeFileLocally(id, filename_2_store)

