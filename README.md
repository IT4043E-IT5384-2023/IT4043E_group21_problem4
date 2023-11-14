# group21_problem4
In order to profile the user wallet, we divide the task into 2 main tasks:

- Cluster user wallet based on the type of projects they join in then label them as an entity.
- Identify these entity behavior (pattern) among these projects.

More specifically, we consider to take  the transaction database, project database and smart contract database from Centic.

Step query:
 - We retrieve the contract address of the project in order to identify that if a project is still working as well as collect their activity frequency and their latest action.
    -  The chain ID helps to identify which chain the project is deployed to. Common chain IDs include.
        - 0x1: Etherium
        - 0x38: Binance smart chain
        - 0x89: Polygon
    - Then we agree to get all projects in that 3 chains.
    - Some projects contain the social media links.
    - Also, with the project related to NFT and Game, we collect some significant fields such as:
        - number of owners
        - number of items
        - rank NFT

- Therefore, we consider to take user information whom join in the project in that chain.
    - In detailed, we get the transaction which sender is user and reciever is the address of contract.


Data fields:
As our main point is to analyze user behaviour on their project so we decided to take transaction data as our main data to analyze (More additional data will be updated), then we take the transfer event of group of user who consider to be the same project or group to analyze their behavior.

Our "transaction" data will have these main fields:

- _id: id of transaction
"transaction_0x54192759ebe761e0dbfab4b872accfea77d010a70c13a8236e7e6265…"


- from_address: sender

- to_address: receiver

- gas


- gas_price

- input


- block_timestamp


- block_number


- item_timestamp

Our "transfer even"t" data will have these main fields:
 - contract address
 - from address
 - to address
 - value


Moreover we also crawl social data to connect to the user wallet address. We crawl data from: Questn to get user wallet social information.

Details:
- idQuestN: The list of user’s id in questn (as 1 user might have many id if they have different accounts so I collect it into a list).
- address: THe list of wallet’s addresses (Like _id 1 user might have many wallet’s addresses).
twitter
- twitter/discord/telegram address.

