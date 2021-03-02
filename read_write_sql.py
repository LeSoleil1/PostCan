from dataAccess import companySQL, videoSQL,jsonSQL,MLModelSQL,userSQL,BLogic


mode = {1:'Video', 2:'Json', 3:'Company',4:'Model',5: 'User'}
while True:
    modeInput = int(input(f"\
        1 -> Working with {mode[1]}\n\
        2 -> Working with {mode[2]}\n\
        3 -> Working with {mode[3]}\n\
        4 -> Working with {mode[4]}\n\
        5 -> Working with {mode[5]}\n"))
    if modeInput in mode.keys():
        if modeInput == 1:
            userInput = int(input(f"\
            1 -> Insert {mode[modeInput]}\n\
            2 -> Find {mode[modeInput]} by ID\n\
            3 -> Alter {mode[modeInput]}\n\
            4 -> Store {mode[modeInput]} locally\n\
            5 -> Find {mode[modeInput]} based on date\n\
            6 -> Find {mode[modeInput]} based on filepath\n\
            7 -> Find certain number of {mode[modeInput]}s\n\
            8 -> (Only for Videos) Find the related Json for Videos\n"))
            obj = videoSQL.VideoSQL()
            obj_json = jsonSQL.JsonSQL()
        elif modeInput == 2:
            userInput = int(input(f"\
            0 -> Create {mode[modeInput]} table\n\
            1 -> Insert {mode[modeInput]}\n\
            2 -> Find {mode[modeInput]} by ID\n\
            3 -> Alter {mode[modeInput]}\n\
            4 -> Store {mode[modeInput]} locally\n\
            5 -> Find {mode[modeInput]} based on date\n\
            6 -> Find {mode[modeInput]} based on filepath\n\
            7 -> Find certain number of {mode[modeInput]}s\n"))
            obj =   jsonSQL.JsonSQL()
        elif modeInput == 3:
            userInput = int(input(f"\
            1 -> Insert {mode[modeInput]}\n\
            2 -> Find {mode[modeInput]} by ID\n\
            3 -> Alter {mode[modeInput]}\n\
            7 -> Find certain number of {mode[modeInput]}s\n"))
            obj =   companySQL.CompanySQL()
        elif modeInput == 4:
            userInput = int(input(f"\
            1 -> Insert {mode[modeInput]}\n\
            2 -> Find {mode[modeInput]} by ID\n\
            3 -> Alter {mode[modeInput]}\n\
            4 -> Store {mode[modeInput]} locally\n\
            5 -> Find {mode[modeInput]} based on date\n\
            6 -> Find {mode[modeInput]} based on filepath\n\
            7 -> Find certain number of {mode[modeInput]}s\n"))
            obj =   MLModelSQL.MLModelSQL()
        elif modeInput == 5:
            userInput = int(input(f"\
            1 -> Insert {mode[modeInput]}\n\
            2 -> Find {mode[modeInput]} by username\n\
            3 -> Change password for {mode[modeInput]}\n\
            7 -> Find certain number of {mode[modeInput]}s\n\
            9 -> Check Username and Password of a {mode[modeInput]}s\n"))
            obj =   userSQL.UserSQL()
        
        if userInput == 0:
            obj.createTable()

        elif userInput == 1:
            if modeInput == 5:
                username = input("Enter the username\t")
                password = input("Enter the password\t")
                obj.insert(username,password)
            else:
                userFilePath = input("Enter the input file path\t")
                obj.insertFile(userFilePath)

        elif userInput == 2:
            if modeInput == 5:
                username = input("Enter the username\t")
                obj.retreiveByUsername(username)
            elif modeInput == 3:
                id = input("Enter ID:\t")
                obj.retreiveByID(id)
            else:
                file_ID = input("Enter ID:\t")
                obj.retreiveFileByID(file_ID)

        elif userInput == 4:
            file_ID = input("Enter ID:\t")
            filename = input("Enter filename:\t")
            obj.storeFileLocally(file_ID,filename)

        elif userInput == 3:
            if modeInput == 5:
                username = input("Enter the username\t")
                password = input("Enter the old password\t")
                new_password = input("Enter the new password\t")
                obj.alterPassword(username,password,new_password)
            else:
                file_ID = input("Enter ID:\t")
                filename = input("New file path:\t")
                obj.alterFile(file_ID,filename)

        elif userInput == 5:
            from_Date = input("Enter from date (yyyy-mm-dd): \t")
            to_Date = input("Enter from date (yyyy-mm-dd):\t")
            obj.retreiveFileByDate(from_Date,to_Date)

        elif userInput == 6:
            userFilePath = input("Enter the input file path\t")
            obj.retreiveFileByPath(userFilePath)
        
        elif userInput == 7:
            num_rows = input("Enter the number of rows to be displayed\t")
            if modeInput == 3 or modeInput == 5:
                obj.retreive(num_rows)
            else:
                obj.retreiveFiles(num_rows)
        
        elif userInput == 8:
            call_method = int(input("By ID (pass 1) or by path (pass 2)\t"))
            if call_method == 1:
                video_ID = input("Enter Video ID:\t")
                num_rows = input("Enter the number of rows to be displayed\t")
                obj_json.retreiveJsonFilesVideoID(num_rows,video_ID)
            elif call_method  == 2:
                video_Path = input("Enter the video file path\t")
                BLogic.getLatestJsonFile(video_Path)

        elif userInput == 9:
            if modeInput == 5:
                username = input("Enter the username\t")
                password = input("Enter the password\t")
                res = obj.checkUserPass(username,password)
                print(res)
        else:
            print('Wrong input') 
