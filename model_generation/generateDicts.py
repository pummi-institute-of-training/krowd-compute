import pandas as pd
import re
import json 
import os

OUTPUT_FOLDER = '/Users/sanjeetkumar/Documents/Krowd/Deliveries/London-April2022/'


street_list_old = ["Abbey Wood","Acton Central","Acton Main Line","Albany Park","Alexandra Palace","Amersham","Anerley","Angel Road","Balham","Banstead","Barking","Barnehurst","Barnes","Barnes Bridge","Battersea Park","Beckenham Hill","Beckenham Junction","Bellingham","Belmont","Belvedere","Beirrylands","Bethnal Green","Bexley","Bexleyheath","Bickley","Birkbeck","BlackfriarsLondon","Blackheath","Blackhorse Road","Bowes Park","Brentford","Brentwood","Brimsdown","Brixton","Brockley","Bromley North","Bromley South","Brondesbury","Brondesbury Park","Broxbourne","Bruce Grove","Bushey","Bush Hill Park","Caledonian Road & Barnsbury","Cambridge Heath","Camden","Canada Water","Cannon StreetLondon","Canonbury","Carpenders Park","Carshalton","Carshalton Beeches","Castle Bar Park","Caterham","Catford","Catford Bridge","Chadwell Heath","Chafford Hundred","Chalfont & Latimer","Charing Cross","Charlton","Cheam","Chelsfield","Cheshunt","Chessington North","Chessington South","Chingford","Chipstead","Chislehurst","Chiswick","Chorleywood","City ThameslinkLondon","Clapham High Street","Clapham","Clapton","Clock House","Coulsdon South","Coulsdon Town","Covent Garden (St Martin's Courtyard)","San Carlo","Covent Garden","Crayford","Crews Hill","Cricklewood","Crofton Park","Crouch Hill","Crystal Palace","Dagenham Dock","Dalston Junction","Dalston Kingsland","Dartford","Denmark Hill","Deptford","Drayton Green","Drayton Park","Ealing Broadway","Earlsfield","East Croydon","East Dulwich","Eden Park","Edmonton Green","Elephant & Castle","Elmers End","Elmstead Woods","Elstree & Borehamwood","Eltham","Emerson Park","Enfield Chase","Enfield Lock","Enfield Town","Epsom Downs","Erith","Essex Road","EustonLondon","Ewell East","Ewell West","Falconwood","Farringdon","Feltham","Fenchurch StreetLondon","Finchley Road & Frognal","Finsbury Park","Finsbury Square","Forest Gate","Forest Hill","Fulwell","Gidea Park","Gipsy Hill","Goodmayes","Gordon Hill","Gospel Oak","Grange Park","Grays","Greenford","Greenwich","Grove Park","Gunnersbury","Hackbridge","Hackney Central","Hackney Downs","Hackney Wick","Hadley Wood","Haggerston","Hampstead Heath","Hampton","Hampton Court","Hampton Wick","Hanwell","Harlesden","Harold Wood","Harringay","Harringay Green Lanes","Harrow & Wealdstone","Harrow-on-the-Hill","Hatch End","Haydons Road","Hayes","Hayes & Harlington","Headstone Lane","Heathrow Central","Heathrow Terminal 4","Heathrow Terminal 5","Hendon","Herne Hill","Highams Park","Highbury & Islington","Hither Green","Homerton","Honor Oak Park","Hornsey","Hounslow","Hoxton Square","Hoxton","Ilford","Imperial Wharf","Isleworth","Kenley","Kensal Green","Kensal Rise","Kensington (Olympia)","Kent House","Kentish Town West","Kentish Town","Kenton","Kew Bridge","Kew Gardens","Kidbrooke","Kilburn High Road","King's Cross","Kings Cross","Kingston","Kingswood","Knockholt","Ladywell","Lee","Lewisham","Leyton Midland Road","Leytonstone High Road","Limehouse","Liverpool StreetLondon","Liverpool Street (London)","London BridgeLondon","London Fields","Loughborough Junction","Lower Sydenham","Malden Manor","Manor Park","Maryland","MaryleboneLondon","Maze Hill","Mill Hill Broadway","Mitcham Eastfields","Mitcham Junction","MoorgateLondon","Morden South","Mortlake","Motspur Park","Mottingham","New Barnet","New Beckenham","New Cross","New Cross Gate","New Eltham","New Malden","New Southgate","Norbiton","Norbury","North Dulwich","Northolt Park","North Sheen","Northumberland Park","North Wembley","Norwood Junction","Nunhead","Oakleigh Park","Ockendon","Old StreetLondon","Orpington","PaddingtonLondon","Palmers Green","Peckham Rye","Penge East","Penge West","Petts Wood","Plumstead","Ponders End","Purfleet","Purley","Purley Oaks","Putney","Queen's Park","Queens Road Peckham","Queenstown Road (Battersea)","Rainham","Ravensbourne","Raynes Park","Rectory Road","Reedham","Richmond","Rickmansworth","Riddlesdown","Romford","Rotherhithe","St Helier","St James Street","St Johns","St Margarets","St Mary Cray","St PancrasLondon","Sanderstead","Selhurst","Seven Kings","Seven Sisters","Shadwell","Shenfield","Shepherd's Bush","Shoreditch","Shortlands","Sidcup","Silver Street","Slade Green","South Acton","Southall","South Bermondsey","Southbury","South Croydon","South Greenford","South Hampstead","South Kenton","South Merton","South Ruislip","South Tottenham","Stamford Hill","Stoke Newington","Stonebridge Park","Stoneleigh","Stratford","Stratford International","Strawberry Hill","Streatham","Streatham Common","Streatham Hill","Sudbury & Harrow Road","Sudbury Hill Harrow","Sundridge Park","Surbiton","Surrey Quays","Sutton","Sutton Common","Swanley","Sydenham","Sydenham Hill","Syon Lane","Tadworth","Tattenham Corner","Teddington","Thames Ditton","Theobalds Grove","Thornton Heath","Tolworth","Tooting","Tottenham Hale","Tulse Hill","Turkey Street","Twickenham","Upminster","Upper Holloway","Upper Warlingham","VauxhallLondon","VictoriaLondon","Waddon","Wallington","Waltham Cross","Walthamstow Central","Walthamstow Queen's Road","Wandsworth Common","Wandsworth Road","Wandsworth Town","Wanstead Park","Wapping","WaterlooLondon","Waterloo EastLondon","Watford High Street","Watford Junction","Welling","Wembley Central","Wembley Stadium","West Brompton","West Kensington","Westcombe Park","West Croydon","West Drayton","West Dulwich","West Ealing","West Hampstead Thameslink","West Hampstead","West Ham","West Norwood","West Ruislip","West Sutton","West Wickham","Whitechapel","White Hart Lane","White City","Whitton","Whyteleafe","Whyteleafe South","Willesden Junction","Wimbledon Chase","Winchmore Hill","Woodgrange Park","Woodmansterne","Wood Street","Woolwich Arsenal","Woolwich Dockyard","Worcester Park","Soho","Notting Hill","Oxford Circus","Canary Wharf","St Christopher's Place","Smiths Wapping","Victoria","Haymarket Hotel","Charlotte Street","Charlotte St","Chelsea","Aldwych","Mayfair","Carnaby","Aldgate","City of London","Mile End","Stepney","Bow Street","Bromley by Bow","Poplar","Hackney","Beckton","East Ham","Upton Park","Wanstead","Dalston","Leyton","Leytonstone","Plaistow","Isle of Dogs","Millwall","Canning Town","Docklands","North Woolwich","Victoria Dock","Woolwich","Walthamstow","South Woodford","Woodford","Barbican","Clerkenwell","Finsbury","Holborn","Bishopsgate","Liverpool Street","Moorgate","Fenchurch Street","Monument","Tower Hill","Blackfriars","Fleet Street","St. Paul's","Temple","Barnsbury","Islington","St. Pancras","East Finchley","Finchley","Finchley Central","Finchley Church End","Manor House","Highbury","Tottenham Court Rd","Hampstead","Highgate","Crouch End","Lower Edmonton","Muswell Hill","Wood Green","Friern Barnet","North Finchley","Woodside Park","Southgate","Upper Edmonton","Archway","Tufnell Park","Totteridge","Whetstone","Alexandra Palace/Park","Camden Town","Regent's Park","St. Marylebone","Dollis Hill","Neasden","Willesden","Willesden Green","Belsize Park","Brent Cross","Swiss Cottage","Kilburn","Paddington","Queens Park","Mill Hill","St. John's Wood","Colindale","Kingsbury","The Hyde","Acton","Hammersmith","Wembley","Golders Green","Hampstead Garden Suburb","Bermondsey","Borough","Camberwell","Lambeth","Southwark","Waterloo","Kennington","Peckham","Surrey Docks","Walworth","Croydon","Penge","Upper Norwood","Beckenham","Dulwich","Wandsworth","South Norwood","Upper Sydenham","Thamesmead","Battersea","Belgravia","Brompton","Pimlico","Westminster","Earl's Court","Fulham","Parson's Green","Knightsbridge","South Kensington","Nine Elms","South Lambeth","Vauxhall","North Brixton","Stockwell","World's End","Clapham Junction","Castelnau","East Sheen","Malden","Roehampton","Mitcham","Collier's Wood","Merton","Cottenham Park","West Wimbledon","Wimbledon","Bayswater","Hyde Park","Ealing","Maida Hill","Maida Vale","Warwick Avenue","Ladbroke Grove","North Kensington","Holland Park","Bloomsbury","Gray's Inn","The Strand","Spitalfields","Berkeley Street","Earls Court", "Seven Dials", "Air Street","Devonshire Row","Spring Street","London - Spring Street","Appold Street","Wigmore Street","Bow","Baker Street","Baker St","Cannon Street","Old Street","Old St","Oxford Street","New Oxford Street","Leicester square","Ludgate ","Ludgate Hill","Ludgate Circus","Picton Place","Regent Street","Butler's Wharf","Brick Lane","Westfield","Goodge Street","Goodge St","Queensway","Gloucester","Grafton Way","Park Street","The O2","O2","Marylebone Station","Hays Galleria","Hay's Galleria","Lime Street","Baker Street","Berners Street","Brunswick Square","The Brunswick","Cardinal Place","Clink Street","Ealing Common","Fulham Broadway","Glasshouse Street","Gloucester Road","High Street Kensington","Kensington High Street","Holloway Road","South Lambeth Road","Victoria Wilton Road","Greenwich Peninsula","Park Royal","Shepherds Bush","Tower Bridge","Russell Square","London Bridge","St Paul","St Pauls","Marylebone","Beak Street","Central St. Giles","Central St Giles","Finchley Road","Finchley Rd O2 Centre","One New Change","St Katharine Docks","St. Katharine Docks","Westfield Stratford City","Harvey Nichols","Royal Festival Hall","Selfridges","Bank","Liverpool St","Portobello","Portobello Road","South Bank","Southbank","Warren St","Warren Street","Bond St","Bond Street","Piccadilly","Denmark Street","Curtain Road","Kensington","Aldermanbury","London","Broadgate","Canary Wharf","Chancery Lane","Sloane","Tower Bridge","Piccadilly","O2","City","Smithfield","Hampstead","Restaurant","Southfields","Guildhall","Spitalfields Bar","Plantation Place","Exmouth Market","Fitzrovia","Northcote Road","Union Street","Leicester Square","Soho","Westfield London","Eathai","St Martins Lane"]
street_list_new = ["Tottenham Court Road","Broadway","North End Road","Meard - St","Leadenhall","America Square"]
stop_words = ['Delivery',"Bar"]
street_list = [item.lower() for item in street_list_old + street_list_new]
stop_words = [item.lower() for item in stop_words]

def clean_name(name):
    
    # 1 take the copy original name
    temp_name = name 
    
    # 2 make name in lower
    name = name.lower()
    
    
    # 3. remove street names
    for item in street_list:
        if item in name:
            name = name.replace(item,'').strip()
            
    # 4. remove stop words
    for item in stop_words:
        if item in name.split(' '):
            name = name.replace(item, '').strip()
    
    # 5. remove everything after '-'
    name = name.split('-')[0].strip()
    
    # 6. remove everything after – (not a dash)
    name = name.split("–")[0].strip()
    
    # 7. remove everything between "()"
    name = re.sub("[\(\[].*?[\)\]]", "", name)

    
    
    # 8. remove apostrophe(`) with empty string
    name = name.replace("'s","").strip()
    
    # 9. normalize & to and
    name = name.replace('&',' and ')
    
    # 10. remove exptra spaces
    name = re.sub(' +', ' ', name)
    
    
    # 11. remove special characters
    name = re.sub('[^A-Za-z0-9]+', ' ', name)
    
    # 12. remove special characters
    if name == '':
        return temp_name
    
    return name

def generate_dictionaries(gdf):
    # input gdf has two columns id and name

    # ----- generating listGroupDict.json ---------------------------------
    gdf['modified_name'] = gdf['name'].apply(clean_name)
    listGroupDict_df = pd.DataFrame(gdf.groupby('modified_name')['id'].apply(list)).reset_index()
    listGroupDict_df['group_id'] = listGroupDict_df['id'].apply(lambda x : x[0])
    listGroupDict = dict(zip(listGroupDict_df.group_id,listGroupDict_df.id))
    with open(os.path.join(OUTPUT_FOLDER,"listGroupDict.json"), "w") as fp:
        json.dump(listGroupDict,fp) 

    # ------------ generating groupDict.json ---------------------------------
    namegroupdict = dict(zip(listGroupDict_df.modified_name,listGroupDict_df.group_id))
    gdf['group_id'] = gdf['modified_name'].apply(lambda x : namegroupdict[x])
    groupDict = dict(zip(gdf.id,gdf.group_id))

    with open(os.path.join(OUTPUT_FOLDER,"groupDict.json"), "w") as fp:
        json.dump(groupDict,fp) 



if __name__=='__main__':
    grpdf = pd.read_json(os.path.join(OUTPUT_FOLDER,"london-model-april2022.json"))

    gdf = grpdf[['id','name']]

    generate_dictionaries(gdf)