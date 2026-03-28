from client.client import AEMETClient
def main():
    client = AEMETClient()
    print(client.get_estaciones())
    

if __name__ == "__main__":
    main()
