import time
from pywinauto import Application
import pyautogui
import pyperclip

app = Application(backend="uia").connect(class_name="WeChatMainWndForPC",title="微信")
mainWin = app.top_window()

def SendMsgToWeChat(mainWin, name, msgLineArr):
    pyperclip.copy(name)
    win = None
    for i in range(0,2):
        try:
            mainWin.restore()
            mainWin.set_focus()

            rec = mainWin.rectangle()
            searchx, searchy = (rec.left+120,  rec.top+35)
            pyautogui.moveTo(searchx, searchy)
            pyautogui.click()
            pyautogui.hotkey("ctrl", "v")
            time.sleep(0.5+i*0.2)
            pyautogui.moveTo(searchx, searchy)
            pyautogui.moveTo(searchx, searchy+80)
            pyautogui.doubleClick()
            win = app[name]
            win.set_focus()
            break
        except Exception as e:
            win = None
            print("Send Exception!")

    if win == None:
        print("Send faled!")
        return

    for line in msgLineArr:
        pyautogui.typewrite(line)
        pyautogui.hotkey('shift', 'enter')

    pyautogui.press('enter')
    win.close()

while True:
    name = input("Who? ")
    msg = input("What?\n ")
    msgLineArr = msg.split("$")
    SendMsgToWeChat(mainWin, name, msgLineArr)

