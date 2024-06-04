# main.py

from sqlalchemy import create_engine, text
import json, logging, requests, sys

VERSION = '0.0.1 dev'


class Main:
    def __setup_db__(self):
        commands = ["""
            CREATE TABLE IF NOT EXISTS Messages (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                Message_ID INTEGER NOT NULL,
                Sender TEXT NOT NULL,
                Login TEXT NOT NULL,
                Domain TEXT NOT NULL,
                Subject TEXT NOT NULL,
                DateTime TEXT NOT NULL,
                Body TEXT,
                text_body TEXT,
                html_body TEXT)
            """,
            "CREATE UNIQUE INDEX IF NOT EXISTS Messages_IDX ON Messages(Message_ID, Login, Domain)"
        ]
        self.engine = create_engine(f"sqlite+pysqlite:///{self.configuration['storage']}", echo=logging.getLogger().level <= logging.DEBUG)
        with self.engine.connect() as conn:
            with conn.begin():
                for command in commands:
                    conn.execute(text(command))

    def __init__(self, filename: str) -> None:
        logging.info(f"Reading configuration file {filename}")
        with open(filename, 'r') as f:
            self.configuration = json.load(f)
        self.__setup_db__()

    def __get_json__(self, url: str) -> dict:
        try:
            session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(max_retries=3)
            session.mount('http://', adapter)
            contents = session.get(url).text
            return json.loads(contents)
        finally:
            session.close()

    def __save_message__(self, login: str, domain: str, message: dict) -> None:
        id = message['id']
        contents = self.__get_json__(f"https://www.1secmail.com/api/v1/?action=readMessage&login={login}&domain={domain}&id={id}")
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT ID FROM Messages WHERE Message_ID = :mid AND Login = :log AND Domain = :dom"), {"mid": id, "log": login, "dom": domain}
            )
            if len(result.all()) > 0:
                logging.info(f"- Ignoring message #{id}")
            else:
                sender = message['from']
                subject = message['subject']
                date = message['date']
                logging.info(f"+ New message #{id} from {sender} on {date}: {subject}")
                conn.execute(
                    text("INSERT INTO Messages(Message_ID, Sender, Login, Domain, Subject, DateTime, Body, text_body, html_body)\
                        VALUES(:mid, :sen, :log, :dom, :sub, :dat, :bod, :tex, :htm)"),
                        {"mid": id, "sen": sender, "log": login, "dom": domain, "sub": subject, "dat": date, "bod": contents['body'], "tex": contents['textBody'], "htm": contents['htmlBody']}
                )
                conn.commit()
        
    def __try_mailbox__(self, login: str, domain: str) -> None:
        logging.info(f"Trying {login}@{domain}")
        messages = self.__get_json__(f"https://www.1secmail.com/api/v1/?action=getMessages&login={login}&domain={domain}")
        logging.debug(f"> > {messages}")
        for message in messages:
            self.__save_message__(login, domain, message)
            # 
            # logging.debug(f"> > > {metadata}")
                                         
    def start(self) -> None:
        logging.debug(f"Storage: {self.configuration['storage']}")
        for mailbox in self.configuration['mailboxes']:
            self.__try_mailbox__(mailbox['login'], mailbox['domain'])
        self.engine.dispose()
            

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <configuration file>")
    else:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(lineno)03d %(message)s')
        # logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')
        logging.info(f"{sys.argv[0]} {VERSION}")
        main = Main(sys.argv[1])
        main.start()
