//
//  CDStore.swift
//  CDStore
//
//  Created by Alex on 01.03.2022.
//

//import UIKit
import Foundation
import CoreData


protocol CoreDataChatsDelegate: AnyObject {
    func receiveCoreDataToNameChats(chatNames: [ChatName])
}

protocol CoreDataGetMessageAsyncDelegate: AnyObject {
    func getMessageFromASYNC(messagesAsync: [Message])
}




class CoreDataManager {
    private let modelName: String = "dataBase"

    static var sharedinstance: CoreDataManager? = {
        let instance = CoreDataManager()
        return instance
    }()


     lazy var managedContext: NSManagedObjectContext = {
        return self.storeContainer.viewContext
    }()

     lazy var privateMOC: NSManagedObjectContext = {
        let context = NSManagedObjectContext(concurrencyType: .privateQueueConcurrencyType)
        context.parent = managedContext
        return context
    }()


     lazy var storeContainer: NSPersistentContainer = {
        let container = NSPersistentContainer(name: modelName)
        container.loadPersistentStores { (storeDescription, error) in

            if let nserror = error as? NSError {
                let str = "& Unresolved *** STORE CONTAINER *** error: \(nserror), userInfo: \(nserror.userInfo)"
                print(#function, str)
            }
        }
        return container
    }()

    func saveContext() {
        guard managedContext.hasChanges else {
            return
        }

        do {
            try managedContext.save()
        } catch let nserror as NSError {
            let str = "& Unresolved *** SAVE CONTEXT *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }
    }

}

class CoreDataEngine {
    var moc = NSManagedObjectContext(concurrencyType: .mainQueueConcurrencyType)
    var storeContainer: NSPersistentContainer!


    static var sharedinstance: CoreDataEngine? = {
        let instance = CoreDataEngine()
        return instance
    }()

    init() {
        moc = CoreDataManager.sharedinstance!.managedContext
        storeContainer = CoreDataManager.sharedinstance?.storeContainer
    }

    func bdType() {
        let description = NSPersistentStoreDescription()

        switch description.type {
        case NSSQLiteStoreType:
            print("& TYPE DATA FOR SAVE in coreData SQLite")

        case NSInMemoryStoreType:
            print("& TYPE DATA FOR SAVE in coreData InMemory")

        case NSBinaryStoreType:
            print("& TYPE DATA FOR SAVE in coreData BinaryStore")

        default:
            break
        }
    }
}


class CoreDataEngineLogin: CoreDataEngine {

    static var sharedinstanceLogin: CoreDataEngineLogin? = {
        let instance = CoreDataEngineLogin()
        return instance
    }()

     func saveSignIn(loginEmail: String, isSignIn: Bool, loginName: String, password: String, moc: NSManagedObjectContext) -> Login? {
        let context = moc

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "Login")

        let sort = [NSSortDescriptor(key: #keyPath(Login.loginName), ascending: true)]
        let predicate = NSPredicate(format: "%K == %@ AND %K == %@",
                                    #keyPath(Login.email), loginEmail,
                                    #keyPath(Login.loginName), loginName)

        fetchRequest.sortDescriptors = sort
        fetchRequest.predicate = predicate

        do {
            let fetchResults = try? (context.fetch(fetchRequest) as? [Login])

            if let fetchResults = fetchResults, fetchResults.isEmpty {
                let loginVal = Login(entity: Login.entity(), insertInto: context)
                loginVal.email = loginEmail
                loginVal.isSignIn = isSignIn
                loginVal.loginName = loginName
                loginVal.password = password

                try? context.save()

                return loginVal

            } else {
                return fetchResults?.first as! Login
            }

        } catch let nserror as NSError {
            let str = "& Error *** SAVE SIGN *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }

        return nil
    }

     func readLogin(loginEmail: String, loginName: String, moc: NSManagedObjectContext) -> Login? {
        let context = moc

        let keyPath1 = #keyPath(Login.email)
        let keyPath2 = #keyPath(Login.loginName)

        let predicate = NSPredicate(format: "%K == %@ AND %K == %@",
                                    keyPath1, loginEmail,
                                    keyPath2, loginName)

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "Login")
        fetchRequest.predicate = predicate

        do {
            let fetchResults = try? (context.fetch(fetchRequest) as? [Login])
            if let results = fetchResults?.first {
                return results
            }

        } catch let nserror as NSError {
            let str = "& Error **** READBASE LOGIN *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }

        return nil
    }
}


class CoreDataEngineChats: CoreDataEngine {

    weak var delegate: CoreDataChatsDelegate?

    static var sharedinstanceChats: CoreDataEngineChats? = {
        let instance = CoreDataEngineChats()
        return instance
    }()

     func readChats(loginEmail: String, limit: Int, moc: NSManagedObjectContext) {
        let context = moc

        let keyPathS = #keyPath(ChatName.chatName)
        let keyPath = #keyPath(ChatName.loginRelationship.email)

        let sort = [NSSortDescriptor(key: keyPathS, ascending: true)]
        let predicate = NSPredicate(format: "%K == %@", keyPath, loginEmail)

         let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "ChatName")

         fetchRequest.sortDescriptors = sort
         fetchRequest.predicate = predicate

         fetchRequest.fetchLimit = limit


         do {
             let fetchResults = try? (context.fetch(fetchRequest) as? [ChatName])

             if let chatNames = fetchResults {
                 self.delegate?.receiveCoreDataToNameChats(chatNames: chatNames)
             }

         } catch let nserror as NSError {
             let str = "& Unresolved *** READBASE *** error: \(nserror), userInfo: \(nserror.userInfo)"
             print(#function, str)
         }
     }


     func helperCreateChatsItem(results: [ChatName]?) -> [ChatItemAB] {
        var chatNames: [ChatItemAB] = []
        results?.forEach { (item) in

            var ext = String()
            if let item = item.lastMessage?.abDataRelationship?.allObjects.first as? ABData {

                if let url = item.urlLocal {
                    ext = url.pathExtension

                } else if let url = item.urlFile {
                    ext = url.pathExtension

                } else if let url = item.urlMain {
                    ext = url.pathExtension
                }
            }

            let badge = String(format: "%d", item.badge)

            let chatItemAB = ChatItemAB()
            chatItemAB.chatName = item.chatName ?? String()
            chatItemAB.backgroundColor = item.settingsRelationships?.profileColor as! UIColor
            chatItemAB.dateForLastMessage = item.lastMessage?.date
            chatItemAB.ext = ext
            chatItemAB.forLogin = item.forLoginName ?? String()
            chatItemAB.isSender = item.lastMessage?.isSender ?? false
            chatItemAB.lastMessage = item.lastMessage?.message ?? String()
            chatItemAB.postStatus = item.lastMessage?.postStatus ?? String()
            chatItemAB.badge = badge

            chatNames.append(chatItemAB)
        }

        return chatNames
    }


     func createChat(loginEmail: String,
                             loginName: String,
                             chatName: String,
                             owner: String,
                             moc: NSManagedObjectContext) -> ChatName {
        let context = moc

        let login = CDStore.defaultLogin.readLogin(loginEmail: loginEmail, loginName: loginName, moc: context)
        let chatValue = ChatName(entity: ChatName.entity(), insertInto: context)
        chatValue.loginRelationship = login


        chatValue.owner = owner
        chatValue.chatName = chatName
        chatValue.forLoginName = login?.email


        let colorValue = SettingsChatName(entity: SettingsChatName.entity(), insertInto: context)
        chatValue.settingsRelationships = colorValue

        let red = CGFloat.random(in: 0...1)
        let green = CGFloat.random(in: 0...1)
        let blue = CGFloat.random(in: 0...1)
        colorValue.profileColor = UIColor(red: red, green: green, blue: blue, alpha: 0.75)


        try? context.save()
        return chatValue
    }

     func createFriend(itemChat: ChatName, chatName: String, avtorName: String, moc: NSManagedObjectContext) {
        let context = moc

        let friendValue = Friend(entity: Friend.entity(), insertInto: context)
        friendValue.chatRelationship = itemChat

        friendValue.avtorName = avtorName
        friendValue.chatName = chatName

        let colorValue = Settings(entity: Settings.entity(), insertInto: context)
        friendValue.settingsRelationships = colorValue

        let red = CGFloat.random(in: 0...1)
        let green = CGFloat.random(in: 0...1)
        let blue = CGFloat.random(in: 0...1)
        colorValue.profileColor = UIColor(red: red, green: green, blue: blue, alpha: 0.75)

        try? context.save()
    }


     func deleteChat(email: String, moc: NSManagedObjectContext) {
        let context = moc

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "ChatName")
        fetchRequest.predicate = NSPredicate(format: "%K == %@", #keyPath(ChatName.chatName), email)

        do {
            let fetchResults = try? (context.fetch(fetchRequest) as? [ChatName])
            fetchResults?.forEach { (object) in
                context.delete(object)
            }

            try? context.save()

        } catch let nserror as NSError {
            let str = "& Error *** DELETE CHAT *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }
    }


     func findChat(loginEmail: String, chatName: String, moc: NSManagedObjectContext) -> ChatName? {

        let context = moc

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "ChatName")

        let keyPath1 = #keyPath(ChatName.loginRelationship.email)
        let keyPath2 = #keyPath(ChatName.chatName)
        let predicate = NSPredicate(format: "%K == %@ AND %K == %@",
                                    keyPath1, loginEmail,
                                    keyPath2, chatName)

        fetchRequest.predicate = predicate

        do {
            let fetchResults = try? (context.fetch(fetchRequest) as? [ChatName])
            if let results = fetchResults?.first {
                return results
            }

        } catch let nserror as NSError {
            let str = "& Unresolved *** FIND CHAT *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
            //fatalError(str)
        }

        return nil
    }

     func createChatsList() -> [ChatName] {

        let context = moc
        var array = [ChatName]()

        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail() ?? "nil"

        let keyPath1 = #keyPath(ChatName.loginRelationship.email)
        let keyPath2 = #keyPath(ChatName.chatName)

        let sort = [NSSortDescriptor(key: keyPath2, ascending: true)]
        let predicate = NSPredicate(format: "%K == %@ AND %K != %@",
                                    keyPath1, loginEmail,
                                    keyPath2, loginEmail)

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "ChatName")
        fetchRequest.sortDescriptors = sort
        fetchRequest.predicate = predicate

        do {
            let fetchResults = try? (context.fetch(fetchRequest)) as? [ChatName]
            if let results = fetchResults {
                array = results
            }

        } catch let nserror as NSError {
            let str = "& Unresolved *** CREATE CHAT LIST *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
            //fatalError(str)
        }

        return array
    }

}


class CoreDataEngineMessages: CoreDataEngine {

    weak var delegate: CoreDataGetMessageAsyncDelegate?

    static var sharedinstanceMessages: CoreDataEngineMessages? = {
        let instance = CoreDataEngineMessages()
        return instance
    }()


     func findMessage(loginEmail: String, chatName: String, id: String, moc: NSManagedObjectContext) -> Message? {
        let context = moc

        let keyPath1 = #keyPath(Message.friendRelationships.chatRelationship.loginRelationship.email)
        let keyPath2 = #keyPath(Message.chatName)
        let keyPath3 = #keyPath(Message.id)
        let predicate = NSPredicate(format: "%K == %@ AND %K == %@ AND %K == %@",
                                    keyPath1, loginEmail,
                                    keyPath2, chatName,
                                    keyPath3, id)

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "Message")
        fetchRequest.predicate = predicate

        do {
            let fetchResults = try? (context.fetch(fetchRequest) as? [Message])
            if let message = fetchResults?.first {
                return message
            }

        } catch let nserror as NSError {
            let str = "& Unresolved *** FIND MESSAGE *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }

        return nil
    }


     func creatMessage(getMessageCD: GetMessageCD, moc: NSManagedObjectContext) -> Message? {
        let context = moc

        let chatName = getMessageCD.chatName ?? String()
        let avtorName = getMessageCD.avtorName ?? String()
        let id = getMessageCD.id ?? String()

        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail() ?? "nil"

        let itemChat = CDStore.defaultChats.findChat(loginEmail: loginEmail, chatName: chatName, moc: moc)

        if let findmessage = findMessage(loginEmail: loginEmail,
                                         chatName: chatName,
                                         id: id,
                                         moc: moc) {

            return nil
        }

        let keyPath1 = #keyPath(Friend.chatRelationship.loginRelationship.email)
        let keyPath2 = #keyPath(Friend.chatName)
        let keyPath3 = #keyPath(Friend.avtorName)

        let predicate = NSPredicate(format: "%K == %@ AND %K == %@ AND %K == %@",
                                    keyPath1, loginEmail,
                                    keyPath2, chatName,
                                    keyPath3, avtorName)

        let fetchRequestFriend = NSFetchRequest<NSFetchRequestResult>(entityName: "Friend")
        fetchRequestFriend.predicate = predicate

        do {
            let fetchResultsFriend = try? (context.fetch(fetchRequestFriend) as? [Friend])
            let friendValue: Friend!


            if let friend = fetchResultsFriend?.first {
                friendValue = friend

            } else {
                friendValue = Friend(entity: Friend.entity(), insertInto: context)
                friendValue.chatRelationship = itemChat

                friendValue.chatName = getMessageCD.chatName
                friendValue.avtorName = getMessageCD.avtorName

                let colorValue = Settings(entity: Settings.entity(), insertInto: context)
                friendValue.settingsRelationships = colorValue

                let red = CGFloat.random(in: 0...1)
                let green = CGFloat.random(in: 0...1)
                let blue = CGFloat.random(in: 0...1)
                colorValue.profileColor = UIColor(red: red, green: green, blue: blue, alpha: 0.75)
            }

            let value = Message(entity: Message.entity(), insertInto: context)
            value.friendRelationships = friendValue

            value.chatName = getMessageCD.chatName
            value.avtorName = getMessageCD.avtorName

            value.date = getMessageCD.date
            value.dateForSection = currentTimeOfSectionsCore(date: getMessageCD.date as! NSDate)
            value.id = getMessageCD.id
            value.isSender = getMessageCD.isSender
            value.message = getMessageCD.message
            value.postStatus = getMessageCD.postStatus
            value.v = getMessageCD.v


            let closuresData = { (item: [Data?]) -> Data? in
                if let item = item.first {
                    return item
                }
                return nil
            }

            let closuresIsLoad = { (item: [Bool?]) -> Bool? in
                if let item = item.first {
                    return item ?? false
                }
                return false
            }

            let closuresBytes = { (item: [String?]) -> String? in
                if let item = item.first {
                    return item
                }
                return nil
            }

            let closuresURL = { (item: [URL?]) -> URL? in
                if let item = item.first {
                    return item
                }
                return nil
            }


            var valueData: ABData?

            if let first = getMessageCD.nameAndExt.first, !first.isEmpty {
                valueData = ABData(entity: ABData.entity(), insertInto: context)

                valueData?.messageRelationship = value

                valueData?.dataFile = closuresData(getMessageCD.dataFile)
                valueData?.dataMain = closuresData(getMessageCD.dataMain)
                valueData?.dataTmp = closuresData(getMessageCD.dataTmp)

                valueData?.fileH = getMessageCD.fileH.first!
                valueData?.fileW = getMessageCD.fileW.first!
                valueData?.duration = getMessageCD.duration.first!
                valueData?.isLoad = closuresIsLoad(getMessageCD.isLoad)!
                valueData?.nameAndExt = closuresBytes(getMessageCD.nameAndExt)
                valueData?.totalBytes = closuresBytes(getMessageCD.totalBytes)

                valueData?.urlFile = closuresURL(getMessageCD.urlFile)
                valueData?.urlMain = closuresURL(getMessageCD.urlMain)
                valueData?.urlTmp = closuresURL(getMessageCD.urlTmp)

                valueData?.urlLocal = closuresURL(getMessageCD.urlLocal)
            }


            var valueForward: ForwardMessage?
            var valueForwardData: ForwardABData?

            if !getMessageCD.fChatName.isEmpty {
                valueForward = ForwardMessage(entity: ForwardMessage.entity(), insertInto: context)

                valueForward?.messageRelationship = value

                valueForward?.chatName = getMessageCD.fChatName
                valueForward?.avtorName = getMessageCD.fAvtor
                valueForward?.dateForSection = value.dateForSection
                valueForward?.id = getMessageCD.fId
                valueForward?.isSender = getMessageCD.fIsSender
                valueForward?.message = getMessageCD.fMessage


                valueForwardData = ForwardABData(entity: ForwardABData.entity(), insertInto: context)

                valueForwardData?.forwardMessageRelationship = valueForward

                valueForwardData?.dataTmp = closuresData(getMessageCD.fDataTmp)
                valueForwardData?.dataMain = closuresData(getMessageCD.fDataMain)
                valueForwardData?.dataFile = closuresData(getMessageCD.fDataFile)

                valueForwardData?.fileH = getMessageCD.fFileH.first!
                valueForwardData?.fileW = getMessageCD.fFileW.first!
                valueForwardData?.duration = getMessageCD.fDuration.first!

                valueForwardData?.isLoad = closuresIsLoad(getMessageCD.fIsLoad)!
                valueForwardData?.nameAndExt = closuresBytes(getMessageCD.fNameAndExt)
                valueForwardData?.totalBytes = closuresBytes(getMessageCD.fTotalBytes)

                valueForwardData?.urlTmp = closuresURL(getMessageCD.fUrlTmp)
                valueForwardData?.urlMain = closuresURL(getMessageCD.fUrlMain)
                valueForwardData?.urlFile = closuresURL(getMessageCD.fUrlFile)

                valueForwardData?.urlLocal = closuresURL(getMessageCD.fUrlLocal)
            }

            itemChat?.lastMessage = value

            return value

        } catch let nserror as NSError {
            let str = "& Unresolved *** CREATE MESSAGE *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }

        return nil
    }

     func creatMessageWithData(getMessageCD: GetMessageCD, moc: NSManagedObjectContext) -> Message? {
        let context = moc

        let chatName = getMessageCD.chatName
        let avtorName = getMessageCD.avtorName
        let id = getMessageCD.id

        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail() ?? "nil"

        let itemChat = CDStore.defaultChats.findChat(loginEmail: loginEmail, chatName: chatName, moc: context)

        if let findMmessage = findMessage(loginEmail: loginEmail,
                                          chatName: chatName,
                                          id: id,
                                          moc: context) {
            return nil
        }

        let fetchRequestFriend = NSFetchRequest<NSFetchRequestResult>(entityName: "Friend")

        let keyPathF1 = #keyPath(Friend.chatRelationship.loginRelationship.email)
        let keyPathF2 = #keyPath(Friend.chatName)
        let keyPathF3 = #keyPath(Friend.avtorName)

        let predicateF = NSPredicate(format: "%K == %@ AND %K == %@ AND %K == %@",
                                     keyPathF1, loginEmail,
                                     keyPathF2, chatName,
                                     keyPathF3, avtorName)

        fetchRequestFriend.predicate = predicateF


        let fetchRequestMessage = NSFetchRequest<NSFetchRequestResult>(entityName: "Message")

        let keyPathM1 = #keyPath(Message.friendRelationships.chatRelationship.loginRelationship.email)
        let keyPathM2 = #keyPath(Message.chatName)
        let keyPathM3 = #keyPath(Message.id)

        let predicateM = NSPredicate(format: "%K == %@ AND %K == %@ AND %K == %@",
                                     keyPathM1, loginEmail,
                                     keyPathM2, chatName,
                                     keyPathM3, id)

        fetchRequestMessage.predicate = predicateM


        do {
            let fetchResultsMessage = try? (context.fetch(fetchRequestMessage) as? [Message])

            if let array = fetchResultsMessage, array.isEmpty {
                let fetchResultsFriend = try? (context.fetch(fetchRequestFriend) as? [Friend])
                let friendValue: Friend!


                if let friend = fetchResultsFriend?.first {
                    friendValue = friend

                } else {
                    friendValue = Friend(entity: Friend.entity(), insertInto: context)
                    friendValue.chatRelationship = itemChat

                    friendValue.avtorName = avtorName
                    friendValue.chatName = chatName

                    let colorValue = Settings(entity: Settings.entity(), insertInto: context)
                    friendValue.settingsRelationships = colorValue

                    let red = CGFloat.random(in: 0...1)
                    let green = CGFloat.random(in: 0...1)
                    let blue = CGFloat.random(in: 0...1)
                    colorValue.profileColor = UIColor(red: red, green: green, blue: blue, alpha: 0.75)
                }

                let value = Message(entity: Message.entity(), insertInto: context)
                value.friendRelationships = friendValue

                value.avtorName = avtorName
                value.chatName = chatName

                value.date = getMessageCD.date
                value.dateForSection = currentTimeOfSectionsCore(date: getMessageCD.date as! NSDate)
                value.id = id
                value.isSender = getMessageCD.isSender
                value.message = getMessageCD.message
                value.postStatus = getMessageCD.postStatus


                let closuresData = { (item: [Data?]) -> Data? in
                    if let item = item.first {
                        return item
                    }
                    return nil
                }

                let closuresIsLoad = { (item: [Bool?]) -> Bool? in
                    if let item = item.first {
                        return item ?? false
                    }
                    return false
                }

                let closuresBytes = { (item: [String?]) -> String? in
                    if let item = item.first {
                        return item
                    }
                    return nil
                }

                let closuresURL = { (item: [URL?]) -> URL? in
                    if let item = item.first {
                        return item
                    }
                    return nil
                }


                var valueData: ABData?
                if let first = getMessageCD.nameAndExt.first, !first.isEmpty {
                    valueData = ABData(entity: ABData.entity(), insertInto: context)

                    valueData?.messageRelationship = value

                    valueData?.nameAndExt = closuresBytes(getMessageCD.nameAndExt)
                    valueData?.dataFile = closuresData(getMessageCD.dataFile)
                    valueData?.dataMain = closuresData(getMessageCD.dataMain)
                    valueData?.dataTmp = closuresData(getMessageCD.dataTmp)

                    valueData?.fileH = getMessageCD.fileH.first!
                    valueData?.fileW = getMessageCD.fileW.first!
                    valueData?.duration = getMessageCD.duration.first!
                    valueData?.isLoad = closuresIsLoad(getMessageCD.isLoad)!
                    valueData?.nameAndExt = closuresBytes(getMessageCD.nameAndExt)
                    valueData?.totalBytes = closuresBytes(getMessageCD.totalBytes)

                    valueData?.urlFile = closuresURL(getMessageCD.urlFile)
                    valueData?.urlMain = closuresURL(getMessageCD.urlMain)
                    valueData?.urlTmp = closuresURL(getMessageCD.urlTmp)

                    valueData?.urlLocal = closuresURL(getMessageCD.urlLocal)
                }


                var valueForward: ForwardMessage?
                var valueForwardData: ForwardABData?
                if !getMessageCD.fChatName.isEmpty {
                    valueForward = ForwardMessage(entity: ForwardMessage.entity(), insertInto: context)

                    valueForward?.messageRelationship = value

                    valueForward?.chatName = getMessageCD.fChatName
                    valueForward?.avtorName = getMessageCD.fAvtor
                    valueForward?.dateForSection = value.dateForSection
                    valueForward?.id = getMessageCD.fId
                    valueForward?.isSender = getMessageCD.fIsSender
                    valueForward?.message = getMessageCD.fMessage

                    valueForwardData = ForwardABData(entity: ForwardABData.entity(), insertInto: context)
                    valueForwardData?.forwardMessageRelationship = valueForward
                    valueForwardData?.nameAndExt = closuresBytes(getMessageCD.fNameAndExt)
                    valueForwardData?.dataTmp = closuresData(getMessageCD.fDataTmp)
                }

                itemChat?.lastMessage = value

                return value
            }
        } catch let nserror as NSError {
            let str = "& Unresolved *** CREATE MESSAGE WITH DATA *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }

        return nil
    }


     func removeMessage(chatName: String, id: String, moc: NSManagedObjectContext) -> Message? {
        let context = moc

        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail() ?? "nil"

        let keyPath1 = #keyPath(Message.friendRelationships.chatRelationship.loginRelationship.email)
        let keyPath2 = #keyPath(Message.chatName)
        let keyPath3 = #keyPath(Message.id)

        let predicate = NSPredicate(format: "%K == %@ AND %K == %@ AND %K == %@",
                                    keyPath1, loginEmail,
                                    keyPath2, chatName,
                                    keyPath3, id)

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "Message")
        fetchRequest.predicate = predicate


        do {
            let fetchResults = try? (context.fetch(fetchRequest) as? [Message])

            if let message = fetchResults?.first {
                message.message = SettingsTextStruct.removeMessage

                if let items = message.abDataRelationship?.allObjects as? [ABData] {
                    items.forEach { (item) in
                        item.dataTmp = nil
                        item.dataMain = nil
                        item.dataFile = nil

                        item.urlTmp = nil
                        item.urlMain = nil
                        item.urlFile = nil
                        item.urlLocal = nil

                        item.nameAndExt = nil
                        item.totalBytes = nil
                        item.duration = 0.0
                        item.fileH = 0.0
                        item.fileW = 0.0
                        item.isLoad = false

                        item.messageRelationship = nil
                    }
                }

                message.abDataRelationship = nil
                message.forwardMessageRelationship = nil

                return message
            }

        } catch let nserror as NSError {
            let str = "& Unresolved *** REMOVE MESSAGE *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }

        return nil
    }

     func deleteMessagesFromChat(getMessageCD: GetMessageCD) {
        let context = CDStore.defaultMessages.moc

        let keyPath1 = #keyPath(Message.friendRelationships.chatRelationship.loginRelationship.email)
        let keyPath2 = #keyPath(Message.chatName)

        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail() ?? "nil"
        let chatName = getMessageCD.chatName

        let predicate = NSPredicate(format: "%K == %@ AND %K == %@",
                                    keyPath1, loginEmail,
                                    keyPath2, chatName)

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "Message")
        fetchRequest.predicate = predicate


        do {
            let fetchResults = try? (context.fetch(fetchRequest) as? [Message])
            fetchResults?.forEach{ (object) in
                context.delete(object)
            }

            try? context.save()

        } catch let nserror as NSError {
            let str = "& Unresolved *** DELETE MESSAGE FROM CHAT *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }
    }


     func saveUpdate(getMessageCD: GetMessageCD, moc: NSManagedObjectContext) -> Message? {
        let context = moc

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "Message")

        let chatName = getMessageCD.chatName
        let avtorName = getMessageCD.avtorName
        let id = getMessageCD.id
        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail() ?? "nil"

        let keyPath1 = #keyPath(Message.friendRelationships.chatRelationship.loginRelationship.email)
        let keyPath2 = #keyPath(Message.chatName)
        let keyPath3 = #keyPath(Message.avtorName)
        let keyPath4 = #keyPath(Message.id)

        let predicate = NSPredicate(format: "%K == %@ AND %K == %@ AND %K == %@ AND %K == %@",
                                    keyPath1, loginEmail,
                                    keyPath2, chatName,
                                    keyPath3, avtorName,
                                    keyPath4, id)

        fetchRequest.predicate = predicate


        do {
            let fetchResults = try? (context.fetch(fetchRequest) as? [Message])

            if let fetchResults = fetchResults, fetchResults.count != 0 {
                if let old = fetchResults.first?.postStatus,
                   old == XMPPMySettings.notSendStatus ||
                    old == XMPPMySettings.deliveryServerStatus ||
                    old == XMPPMySettings.deliveredClientStatus
                 {
                    fetchResults.first?.postStatus = getMessageCD.postStatus

                    let chatName = getMessageCD.chatName

                    let friends = CDStore.defaultChats.findChat(loginEmail: loginEmail,
                                                                chatName: chatName,
                                                                moc: context)?.friendRelationship

                    friends?.forEach { (friend) in
                        if (friend as! Friend).chatName == chatName &&
                            (friend as! Friend).avtorName == avtorName
                        {
                            (friend as! Friend).chatRelationship?.lastMessage = fetchResults.first
                        }
                    }

                    try? context.save()

                    return fetchResults.first
                 }
            }

        } catch let nserror as NSError {
            let str = "& Unresolved *** SAVE UPDATE *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }

        return nil
    }

     func updateForwardForMessage(getMessageCD: GetMessageCD, moc: NSManagedObjectContext) {
        let context = moc

        let chatName = getMessageCD.chatName
        let avtorName = getMessageCD.avtorName
        let id = getMessageCD.id
        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail() ?? "nil"

        let keyPathM1 = #keyPath(Message.friendRelationships.chatRelationship.loginRelationship.email)
        let keyPathM2 = #keyPath(Message.chatName)
        let keyPathM3 = #keyPath(Message.avtorName)
        let keyPathM4 = #keyPath(Message.id)

        let predicateM = NSPredicate(format: "%K == %@ AND %K == %@ AND %K == %@ AND %K == %@",
                                     keyPathM1, loginEmail,
                                     keyPathM2, chatName,
                                     keyPathM3, avtorName,
                                     keyPathM4, id)

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "Message")
        fetchRequest.predicate = predicateM


        let keyPathF1 =
         #keyPath(ForwardABData.forwardMessageRelationship.messageRelationship.friendRelationships.chatRelationship.loginRelationship.email)
        let keyPathF2 = #keyPath(ForwardABData.forwardMessageRelationship.messageRelationship.chatName)
        let keyPathF3 = #keyPath(ForwardABData.forwardMessageRelationship.messageRelationship.id)


        let predicateF = NSPredicate(format: "%K == %@ AND %K == %@ AND %K == %@",
                                     keyPathF1, loginEmail,
                                     keyPathF2, chatName,
                                     keyPathF3, id)

        let fetchRequestData = NSFetchRequest<NSFetchRequestResult>(entityName: "ForwardABData")
        fetchRequestData.predicate = predicateF


        do {
            let fetchResults = try? (context.fetch(fetchRequest) as? [Message])
            let fetchResultsData = try? (context.fetch(fetchRequestData) as? [ForwardABData])

            if let first = fetchResults?.first, let firstData = fetchResultsData?.first {

                if let date = getMessageCD.date {
                    first.date = date
                }
                if let dataFile = getMessageCD.fDataFile.first as? Data {
                    firstData.dataFile = dataFile
                }
                if let  dataMain = getMessageCD.fDataMain.first as? Data {
                    firstData.dataMain = dataMain
                }
                if let dataTmp = getMessageCD.fDataTmp.first as? Data {
                    firstData.dataTmp = dataTmp
                }
                if let isLoad = getMessageCD.fIsLoad.first as? Bool {
                    firstData.isLoad = isLoad
                }

                if let urlTmp = getMessageCD.fUrlTmp.first as? URL {
                    firstData.urlTmp = urlTmp
                }
                if let urlMain = getMessageCD.fUrlMain.first as? URL {
                    firstData.urlMain = urlMain
                }
                if let urlFile = getMessageCD.fUrlFile.first as? URL {
                    firstData.urlFile = urlFile
                }
                if let urlLocal = getMessageCD.fUrlLocal.first as? URL {
                    firstData.urlLocal = urlLocal
                }

                try? context.save()

            }

        } catch let nserror as NSError {
            let str = "& Unresolved *** UPDATE MAIN FOR MESSAGE *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }
    }

     func updateMainForMessage(getMessageCD: GetMessageCD, moc: NSManagedObjectContext) {
        let context = moc

        let chatName = getMessageCD.chatName
        let avtorName = getMessageCD.avtorName
        let id = getMessageCD.id
        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail() ?? "nil"

        let keyPathM1 = #keyPath(Message.friendRelationships.chatRelationship.loginRelationship.email)
        let keyPathM2 = #keyPath(Message.chatName)
        let keyPathM3 = #keyPath(Message.avtorName)
        let keyPathM4 = #keyPath(Message.id)

        let predicateM = NSPredicate(format: "%K == %@ AND %K == %@ AND %K == %@ AND %K == %@",
                                     keyPathM1, loginEmail,
                                     keyPathM2, chatName,
                                     keyPathM3, avtorName,
                                     keyPathM4, id)

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "Message")
        fetchRequest.predicate = predicateM


        let keyPathD1 = #keyPath(ABData.messageRelationship.friendRelationships.chatRelationship.loginRelationship.email)
        let keyPathD2 = #keyPath(ABData.messageRelationship.chatName)
        let keyPathD3 = #keyPath(ABData.messageRelationship.avtorName)
        let keyPathD4 = #keyPath(ABData.messageRelationship.id)

        let predicateD = NSPredicate(format: "%K == %@ AND %K == %@ AND %K == %@ AND %K == %@",
                                     keyPathD1, loginEmail,
                                     keyPathD2, chatName,
                                     keyPathD3, avtorName,
                                     keyPathD4, id)

        let fetchRequestData = NSFetchRequest<NSFetchRequestResult>(entityName: "ABData")
        fetchRequestData.predicate = predicateD


        let keyPathF1 =
         #keyPath(ForwardABData.forwardMessageRelationship.messageRelationship.friendRelationships.chatRelationship.loginRelationship.email)
        let keyPathF2 = #keyPath(ForwardABData.forwardMessageRelationship.messageRelationship.chatName)
        let keyPathF3 = #keyPath(ForwardABData.forwardMessageRelationship.id)


        let predicateF = NSPredicate(format: "%K == %@ AND %K == %@ AND %K == %@",
                                     keyPathF1, loginEmail,
                                     keyPathF2, chatName,
                                     keyPathF3, id)

        let fetchRequestDataF = NSFetchRequest<NSFetchRequestResult>(entityName: "ForwardABData")
        fetchRequestDataF.predicate = predicateF


        do {
            let fetchResults = try? (context.fetch(fetchRequest) as? [Message])
            let fetchResultsData = try? (context.fetch(fetchRequestData) as? [ABData])
            let fetchResultsDataF = try? (context.fetch(fetchRequestDataF) as? [ForwardABData])

            if let first = fetchResults?.first, let firstData = fetchResultsData?.first {

                var forwardData: ForwardABData?
                if let forwardFirst = fetchResultsDataF?.first {
                    forwardData = forwardFirst

                    print(#function," & array:\(fetchResultsDataF?.count)")

                }

                if let date = getMessageCD.date {
                    first.date = date
                }
                if let dataFile = getMessageCD.dataFile.first as? Data {
                    firstData.dataFile = dataFile
                }
                if let  dataMain = getMessageCD.dataMain.first as? Data {
                    firstData.dataMain = dataMain

                    fetchResultsDataF?.forEach { (i) in
                        i.dataMain = dataMain
                    }

                }
                if let dataTmp = getMessageCD.dataTmp.first as? Data {
                    firstData.dataTmp = dataTmp

                    fetchResultsDataF?.forEach { (i) in
                        i.dataTmp = dataTmp
                    }

                }
                if let isLoad = getMessageCD.isLoad.first as? Bool {
                    firstData.isLoad = isLoad

                    fetchResultsDataF?.forEach { (i) in
                        i.isLoad = isLoad
                    }

                }

                if let urlTmp = getMessageCD.urlTmp.first as? URL {
                    firstData.urlTmp = urlTmp
                }
                if let urlMain = getMessageCD.urlMain.first as? URL {
                    firstData.urlMain = urlMain
                }
                if let urlFile = getMessageCD.urlFile.first as? URL {
                    firstData.urlFile = urlFile
                }
                if let urlLocal = getMessageCD.urlLocal.first as? URL {
                    firstData.urlLocal = urlLocal
                }

                try? context.save()

            }

        } catch let nserror as NSError {
            let str = "& Unresolved *** UPDATE MAIN FOR MESSAGE *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }
    }

     func saveUpdateDate(getMessageCD: GetMessageCD, moc: NSManagedObjectContext) -> Bool {
        let context = moc

        let chatName = getMessageCD.chatName
        let avtorName = getMessageCD.avtorName
        let id = getMessageCD.id

        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail() ?? "nil"

        let itemChat = CDStore.defaultChats.findChat(loginEmail: loginEmail, chatName: chatName, moc: context)

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "Message")

        let keyPath1 = #keyPath(Message.friendRelationships.chatRelationship.loginRelationship.email)
        let keyPath2 = #keyPath(Message.chatName)
        let keyPath3 = #keyPath(Message.avtorName)
        let keyPath4 = #keyPath(Message.id)

        let predicate = NSPredicate(format: "%K == %@ AND %K == %@ AND %K == %@ AND %K == %@",
                                    keyPath1, loginEmail,
                                    keyPath2, chatName,
                                    keyPath3, avtorName,
                                    keyPath4, id)

        fetchRequest.predicate = predicate


        do {
            let fetchResults = try? (context.fetch(fetchRequest) as? [Message])

            if let fetchResults = fetchResults, fetchResults.count != 0 {
                if let old = fetchResults.first?.postStatus,
                   old == XMPPMySettings.notSendStatus
                 {

                    fetchResults.first?.date = getMessageCD.date
                    fetchResults.first?.id = getMessageCD.idNew

                    itemChat?.friendRelationship?.forEach { (friend) in
                        if (friend as! Friend).chatName == chatName &&
                            (friend as! Friend).avtorName == avtorName
                        {
                            (friend as! Friend).chatRelationship?.lastMessage = fetchResults.first
                        }
                    }

                    try? context.save()
                    return true

                }
            }
        } catch let nserror as NSError {
            let str = "& Unresolved *** SAVE UPDATE DATE *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)

        }

        return false
    }


     func lostMessage(getMessageCD: GetMessageCD, moc: NSManagedObjectContext) -> [Message]? {
        let context = moc

        let chatName = getMessageCD.chatName
        let avtorName = getMessageCD.avtorName
        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail() ?? "nil"

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "Message")
        fetchRequest.sortDescriptors = [NSSortDescriptor(key: #keyPath(Message.date), ascending: true)]

        let keyPath1 = #keyPath(Message.friendRelationships.chatRelationship.loginRelationship.email)
        let keyPath2 = #keyPath(Message.chatName)
        let keyPath3 = #keyPath(Message.avtorName)
        let keyPath4 = #keyPath(Message.postStatus)

        let predicate = NSPredicate(format: "%K == %@ AND %K == %@ AND %K == %@ AND %K != %@",
                                    keyPath1, loginEmail,
                                    keyPath2, chatName,
                                    keyPath3, avtorName,
                                    keyPath4, XMPPMySettings.readStatus)

        fetchRequest.predicate = predicate


        do {
            let fetchResults = try? (context.fetch(fetchRequest) as? [Message])
            fetchResults?.forEach { (i) in
                let str = String(format: "& message: %@, id: %@, avtor: %@, chat: %@", i.message!, i.id!, i.avtorName!, i.chatName!)
                print(str)
            }

            return fetchResults

        } catch let nserror as NSError {
            let str = "& Unresolved *** LOST MESSAGE *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }

        return nil
    }



     func saveMessageArhive(getMessageCD: GetMessageCD, moc: NSManagedObjectContext) -> Bool {
        let context = moc

        let chatName = getMessageCD.chatName
        let avtorName = getMessageCD.avtorName
        let date = getMessageCD.date
        let id = getMessageCD.id
        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail() ?? "nil"

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "Message")

        let keyPathM1 = #keyPath(Message.friendRelationships.chatRelationship.loginRelationship.email)
        let keyPathM2 = #keyPath(Message.chatName)
        let keyPathM3 = #keyPath(Message.avtorName)
        let keyPathM4 = #keyPath(Message.date)
        let keyPathM5 = #keyPath(Message.id)

        let predicateM = NSPredicate(format: "%K == %@ AND %K == %@ AND %K == %@ AND %K == %@ AND %K == %@",
                                     keyPathM1, loginEmail,
                                     keyPathM2, chatName,
                                     keyPathM3, avtorName,
                                     keyPathM4, date as! NSDate,
                                     keyPathM5, id)

        fetchRequest.predicate = predicateM


        let fetchRequestFriend = NSFetchRequest<NSFetchRequestResult>(entityName: "Friend")

        let keyPathF1 = #keyPath(Friend.chatRelationship.loginRelationship.email)
        let keyPathF2 = #keyPath(Friend.chatName)
        let keyPathF3 = #keyPath(Friend.avtorName)

        let predicateF = NSPredicate(format: "%K == %@ AND %K == %@ AND %K == %@",
                                     keyPathF1, loginEmail,
                                     keyPathF2, chatName,
                                     keyPathF3, avtorName)

        fetchRequestFriend.predicate = predicateF

        fetchRequestFriend.fetchLimit = 13_00

        do {
            let fetchResults = try? (context.fetch(fetchRequest) as? [Message])
            let fetchResultsFriend = try? (context.fetch(fetchRequestFriend) as? [Friend])


            if let fetchResults = fetchResults, fetchResults.count != 0 {

            } else {
                if let friend = fetchResultsFriend?.first {
                    let value = Message(entity: Message.entity(), insertInto: context)

                    if friend.chatName == chatName {

                        value.friendRelationships = friend

                        value.chatName = chatName
                        value.avtorName = avtorName
                        value.date = date
                        value.dateForSection = currentTimeOfSectionsCore(date: date as! NSDate)
                        value.id = id
                        value.isSender = getMessageCD.isSender
                        value.message = getMessageCD.message
                        value.postStatus = getMessageCD.postStatus

                        friend.chatRelationship?.lastMessage = value
                    }

                    try? context.save()
                    return true
                }
            }
        } catch let nserror as NSError {
            let str = "& Unresolved *** SAVE MESSAGE ARCHIVE *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }

        return false
    }

     func scrollWriteChatName(chatName: String, section: Int, row: Int, moc: NSManagedObjectContext) {

        let context = moc

        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail() ?? "nil"

        let keyPath1 = #keyPath(ChatName.loginRelationship.email)
        let keyPath2 = #keyPath(ChatName.chatName)
        let predicate = NSPredicate(format: "%K == %@ AND %K == %@",
                                    keyPath1, loginEmail,
                                    keyPath2, chatName)

        let fetchRequestChatName = NSFetchRequest<NSFetchRequestResult>(entityName: "ChatName")
        fetchRequestChatName.predicate = predicate

        do {
            let fetchResultsChatName = try? (context.fetch(fetchRequestChatName) as? [ChatName])
            if let first = fetchResultsChatName?.first?.settingsRelationships {
                first.section = Int64(section)
                first.row = Int64(row)

                try? context.save()
            }

        } catch let nserror as NSError {
            let str = "& Unresolved *** WRITE SCROLL POSITION FOR MESSAGE CHAT *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }
    }

     func scrollReadChatName(chatName: String, moc: NSManagedObjectContext) -> ChatName? {
        let context = moc

        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail() ?? "nil"

        let keyPath1 = #keyPath(ChatName.loginRelationship.email)
        let keyPath2 = #keyPath(ChatName.chatName)
        let predicate = NSPredicate(format: "%K == %@ AND %K == %@",
                                    keyPath1, loginEmail,
                                    keyPath2, chatName)

        let fetchRequestChatName = NSFetchRequest<NSFetchRequestResult>(entityName: "ChatName")
        fetchRequestChatName.predicate = predicate

        do {
            let fetchResultsChatName = try? (context.fetch(fetchRequestChatName) as? [ChatName])
            if let chat = fetchResultsChatName?.first {
                return chat
            }

        } catch let nserror as NSError {
            let str = "& Unresolved *** Read SCROLL POSITION FOR MESSAGE CHAT *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }

        return nil
    }
}



extension CoreDataEngineMessages {

    func sortForTestArray(chatName: String, limit: Int, moc: NSManagedObjectContext) -> [Message] {
        let context = moc
        var sorted = [Message]()

        let keyPathS = #keyPath(Message.date)
        let keyPathP = #keyPath(Message.chatName)

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "Message")
        let sort = [NSSortDescriptor(key: keyPathS, ascending: false)]
        let predicate =  NSPredicate(format: "%K == %@", keyPathP, chatName)

        fetchRequest.sortDescriptors = sort
        fetchRequest.predicate = predicate
        fetchRequest.fetchLimit = limit
        fetchRequest.fetchBatchSize = 20

        do {
            let arraySorted = try? (context.fetch(fetchRequest)) as? [Message]

            if let sort = arraySorted {
                sorted = sort
            }

        } catch let nserror as NSError {
            let str = "& Unresolved *** SORT MESSAGES *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)

        }

        return sorted
    }

    func sortMessages(loginEmail: String, chatName: String, limit: Int, moc: NSManagedObjectContext) -> [Message] {
        let context = moc
        var sorted = [Message]()

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "Message")

        let keyPathS = #keyPath(Message.date)
        let keyPath1 = #keyPath(Message.friendRelationships.chatRelationship.loginRelationship.email)
        let keyPath2 = #keyPath(Message.chatName)
        let keyPath3 = #keyPath(Message.message)
        let wasRemove = SettingsTextStruct.removeMessage

        let sort = [NSSortDescriptor(key: keyPathS, ascending: false)]
        let predicate =  NSPredicate(format: "%K == %@ AND %K == %@ AND %K != %@",
                                     keyPath1, loginEmail,
                                     keyPath2, chatName,
                                     keyPath3, wasRemove)


        fetchRequest.sortDescriptors = sort
        fetchRequest.predicate = predicate
        fetchRequest.fetchLimit = limit
        fetchRequest.fetchBatchSize = 20

        do {
            let arraySorted = try? (context.fetch(fetchRequest)) as? [Message]

            if let sort = arraySorted {
                sorted = sort
            }

        } catch let nserror as NSError {
            let str = "& Unresolved *** SORT MESSAGES *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }

        return sorted
    }

    func asyncSortMessages(email: String, chatName: String, limit: Int, moc: NSManagedObjectContext) {
        let context = moc

        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail() ?? "nil"

        let keyPathS = #keyPath(Message.date)
        let keyPath1 = #keyPath(Message.friendRelationships.chatRelationship.loginRelationship.email)
        let keyPath2 = #keyPath(Message.chatName)
        let keyPath3 = #keyPath(Message.message)

        let wasRemove = SettingsTextStruct.removeMessage

        let sort = [NSSortDescriptor(key: keyPathS, ascending: false)]
        let predicate =  NSPredicate(format: "%K == %@ AND %K == %@ AND %K != %@",
                                     keyPath1, loginEmail,
                                     keyPath2, chatName,
                                     keyPath3, wasRemove)

        let fetchRequest: NSFetchRequest<Message> = Message.fetchRequest()
        fetchRequest.sortDescriptors = sort
        fetchRequest.predicate = predicate
        fetchRequest.fetchLimit = limit
        fetchRequest.fetchBatchSize = 20


        let asyncFetchRequest = NSAsynchronousFetchRequest<Message>(fetchRequest: fetchRequest) { [unowned self] (result: NSAsynchronousFetchResult) in

            guard let messages = result.finalResult else { return }
            print(#function," & message count=\(messages.count)")

            self.delegate?.getMessageFromASYNC(messagesAsync: messages)
        }

        do {
            try context.execute(asyncFetchRequest)

        } catch let nserror as NSError {
            let str = "& Unresolved *** ASYNC SORT MESSAGES *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }
    }
}


class CoreDataEngineTasks: CoreDataEngine {

    static var sharedinstanceTasks: CoreDataEngineTasks? = {
        let instance = CoreDataEngineTasks()
        return instance
    }()


     func findTask(user: String, moc: NSManagedObjectContext) -> ParentAndChildren? {
        let context = moc

        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail() ?? "nil"

        let keyPath1 = #keyPath(ParentAndChildren.aLoginRelationship.email)
        let keyPath2 = #keyPath(ParentAndChildren.name)

        let predicate = NSPredicate(format: "%K == %@ AND %K == %@",
                                    keyPath1, loginEmail,
                                    keyPath2, user)

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "ParentAndChildren")
        fetchRequest.predicate = predicate

        do {
            let results = try? (context.fetch(fetchRequest) as? [ParentAndChildren])
            if let item = results?.first {
                return item
            }

        } catch {
            print(#function, " Error: \(error)! Description: \(error.localizedDescription)")
        }

        return nil
    }

     func createChatForTask(itemTask: ParentAndChildren,
                                    loginEmail: String,
                                    loginName: String,
                                    chatName: String,
                                    owner: String,
                                    moc: NSManagedObjectContext) -> ChatName {

        let context = moc

        if let chat = CDStore.defaultChats.findChat(loginEmail: loginEmail, chatName: chatName, moc: moc) {
            return chat
        }

        let login = CDStore.defaultLogin.readLogin(loginEmail: loginEmail, loginName: loginName, moc: context)
        let chatValue = ChatName(entity: ChatName.entity(), insertInto: context)
        chatValue.loginRelationship = login
        chatValue.taskRelationship = itemTask

        chatValue.owner = owner
        chatValue.chatName = chatName
        chatValue.forLoginName = login?.email

        let colorValue = SettingsChatName(entity: SettingsChatName.entity(), insertInto: context)
        chatValue.settingsRelationships = colorValue

        let red = CGFloat.random(in: 0...1)
        let green = CGFloat.random(in: 0...1)
        let blue = CGFloat.random(in: 0...1)
        colorValue.profileColor = UIColor(red: red, green: green, blue: blue, alpha: 0.75)


        try? context.save()
        return chatValue
    }

     func createFriendForTask(itemChat: ChatName, chatName: String, avtorName: String, moc: NSManagedObjectContext) {
        let context = moc

        let friendValue = Friend(entity: Friend.entity(), insertInto: context)
        friendValue.chatRelationship = itemChat

        friendValue.avtorName = avtorName
        friendValue.chatName = chatName

        let colorValue = Settings(entity: Settings.entity(), insertInto: context)
        friendValue.settingsRelationships = colorValue

        let red = CGFloat.random(in: 0...1)
        let green = CGFloat.random(in: 0...1)
        let blue = CGFloat.random(in: 0...1)
        colorValue.profileColor = UIColor(red: red, green: green, blue: blue, alpha: 0.75)

        try? context.save()
    }

     func createSecretChat(itemTask: ParentAndChildren,
                                   chatName: String,
                                   owner: String,
                                   moc: NSManagedObjectContext) -> ChatName {
        let context = moc
        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail() ?? "nil"
        let loginName = read.readDefaultLogin() ?? "nil"

        let secretValue = SecretChat(entity: SecretChat.entity(), insertInto: context)

        let itemChat = createChatForTask(itemTask: itemTask,
                                         loginEmail: loginEmail,
                                         loginName: loginName,
                                         chatName: chatName,
                                         owner: owner,
                                         moc: context)

        secretValue.taskRelationship = itemTask
        secretValue.chatNameRelationship = itemChat

        secretValue.owner = owner
        secretValue.chatName = chatName
        secretValue.forLoginName = loginEmail

        try? context.save()

        return itemChat
    }

    func saveTaskCD(jsonVCard: DataJsonVCard,
                    dataPng: Data,
                    adress: String,
                    user: String,
                    moc: NSManagedObjectContext) -> ParentAndChildren? {
        let context = moc

        let read = ReadAndSaveDefaultFrom()
        guard let loginEmail = read.readDefaultEmail() else { return nil }

        let keyPath1 = #keyPath(ParentAndChildren.aLoginRelationship.email)
        let keyPath2 = #keyPath(ParentAndChildren.name)

        let predicate = NSPredicate(format: "%K == %@ AND %K == %@",
                                    keyPath1, loginEmail,
                                    keyPath2, user)

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "ParentAndChildren")
        fetchRequest.predicate = predicate

        do {
            let results = try? (context.fetch(fetchRequest) as? [ParentAndChildren])

            if let children = results?.first {

                if let parentKey = jsonVCard.parentKey {

                    if let parent = addRelationship(parentKey: parentKey, children: children, moc: context),
                       let isContains = parent.parentRelationship?.contains(children),
                       !isContains {

                        let level: Int64 = parent.level + 1
                        parent.isExpanded = true


                        children.childrenRelationship = parent
                        children.colorEnd = parent.colorStart
                        children.groupNameForSorted = parent.name
                        children.level = level


                        let dateChild = children.date
                        let dateParent = parent.date
                        if dateChild! > dateParent! {
                            parent.date = dateChild
                        }

                        recursingChangeLevel(children: children)

                        do {
                            try context.save()

                            return children

                        } catch let nserror as NSError {

                            let str = "& Unresolved *** SAVE TASKS UPDATE ITEM *** error: \(nserror), userInfo: \(nserror.userInfo)"
                            print(#function, str)

                        }
                    }
                }

                return children

            } else {
                let val = createItem(dataJSON: jsonVCard, dataPng: dataPng, adress: adress, user: user, moc: context)

                do {
                    try context.save()
                    return val

                } catch let nserror as NSError {
                    let str = "& Unresolved *** SAVE TASKS NEW ITEM *** error: \(nserror), userInfo: \(nserror.userInfo)"
                    print(#function, str)

                }
            }

        } catch let nserror as NSError {
            let str = "& Unresolved *** SAVE TASKS NOT ERROR RESULTS TRY? *** error: \(nserror), userInfo: \(nserror.userInfo)"
            print(#function, str)
        }

        return nil
    }

    func createItem(dataJSON: DataJsonVCard,
                    dataPng: Data,
                    adress: String,
                    user: String,
                    moc: NSManagedObjectContext) -> ParentAndChildren {
        let context = moc

        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail()!
        let loginName = read.readDefaultLogin()!

        let login = CDStore.defaultLogin.readLogin(loginEmail: loginEmail,
                                                   loginName: loginName,
                                                   moc: context)

        let color = SettingsImageStruct.randColors()

        let val = ParentAndChildren(entity: ParentAndChildren.entity(), insertInto: context)

        val.image = dataPng
        val.taskName = dataJSON.name
        val.adress = adress
        val.companyKey = dataJSON.companyKey
        val.state = dataJSON.state ?? 0
        val.stateDescription = dataJSON.stateDescription
        val.colorStart = UIColor(red: color[0], green: color[1], blue: color[2])
        val.colorEnd = val.colorStart
        val.isHide = false
        val.isMiddle = false
        val.isExpanded = false
        val.name = user
        val.date = Date(milliseconds: dataJSON.timestamp!)

        val.groupNameForSorted = user
        val.level = 0

        val.aLoginRelationship = login

        if let parentKey = dataJSON.parentKey {
            if let parent = addRelationship(parentKey: parentKey, children: val, moc: context),
               let isContains = parent.parentRelationship?.contains(val),
               !isContains {

                parent.isExpanded = true
                val.childrenRelationship = parent
                val.colorEnd = parent.colorStart
                val.groupNameForSorted = parent.name
                val.level = parent.level + 1
            }
        }

        return val
    }

    func addRelationship(parentKey: String, children: ParentAndChildren, moc: NSManagedObjectContext) -> ParentAndChildren? {
        let context = moc
        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail()!

        let keyPath1 = #keyPath(ParentAndChildren.aLoginRelationship.email)
        let keyPath2 = #keyPath(ParentAndChildren.name)

        let predicate = NSPredicate(format: "%K == %@ AND %K == %@",
                                    keyPath1, loginEmail,
                                    keyPath2, parentKey)


        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "ParentAndChildren")
        fetchRequest.predicate = predicate

        if let results = try? (context.fetch(fetchRequest) as? [ParentAndChildren]),
           let parent = results.first {
            return parent
        }

        return nil
    }


    func recursingChangeLevel(children: ParentAndChildren) {
        children.parentRelationship?.forEach { (i) in
            let child = i as! ParentAndChildren
            child.colorEnd = children.colorStart
            child.groupNameForSorted = children.groupNameForSorted
            child.level += 1

            recursingChangeLevel(children: child)
        }
    }


    func readTaskCD(moc: NSManagedObjectContext) -> [ParentAndChildren] {
        let context = moc

        let read = ReadAndSaveDefaultFrom()
        let loginEmail = read.readDefaultEmail() ?? "nil"

        let keyPath1 = #keyPath(ParentAndChildren.aLoginRelationship.email)

        let predicate = NSPredicate(format: "%K == %@",
                                    keyPath1, loginEmail)

        let sortByDate = NSSortDescriptor(key: #keyPath(ParentAndChildren.date), ascending: false)
        let sortByGroup = NSSortDescriptor(key: #keyPath(ParentAndChildren.groupNameForSorted), ascending: true)
        let sortByLevel = NSSortDescriptor(key: #keyPath(ParentAndChildren.level), ascending: true)

        let fetchRequest = NSFetchRequest<NSFetchRequestResult>(entityName: "ParentAndChildren")
        fetchRequest.sortDescriptors = [sortByDate, sortByGroup, sortByLevel]
        fetchRequest.predicate = predicate

        do {
            let results = try? (context.fetch(fetchRequest) as? [ParentAndChildren])
            if let results = results, !results.isEmpty {
                return results
            }

        } catch {
            print(#function, " Error: \(error)! Description: \(error.localizedDescription)")
        }

        return []
    }
}



class CDStore {

    static var defaultLocal: CoreDataEngine = {
        let instance = CoreDataEngine.sharedinstance!
        return instance
    }()

    static var defaultLogin: CoreDataEngineLogin = {
        let instance = CoreDataEngineLogin.sharedinstanceLogin!
        return instance
    }()

    static var defaultChats: CoreDataEngineChats = {
        let instance = CoreDataEngineChats.sharedinstanceChats!
        return instance
    }()

    static var defaultMessages: CoreDataEngineMessages = {
        let instance = CoreDataEngineMessages.sharedinstanceMessages!
        return instance
    }()

    static var defaultTasks: CoreDataEngineTasks = {
        let instance = CoreDataEngineTasks.sharedinstanceTasks!
        return instance
    }()

}

