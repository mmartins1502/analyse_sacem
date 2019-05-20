#!/usr/bin/env python
# coding: utf-8

# ### NOTE:
#    #### Optimiser le code pour ajouter les ayants droit

# In[1]:


import pandas as pd
from tqdm import tqdm, tnrange, tqdm_notebook
import os
import numpy as np
import time
import threading
import functools
from datetime import timedelta, datetime
import shutil


# # 4. Concatenation du fichier final

# In[2]:


def trou(x):
    if x == "----TROU----":
        return True
    return False

def concatenate_final_csv(task, save_path):
    if input('Souhaitez-vous concaténer les ' + task + ' ?\n') in ['Y', 'y', 'yes', 'Yes', 'oui', 'Oui', 'OUI', 'o', 'O', 'YES']:
           
        if save_path is None:
            folder = input('Renseignez le chemin vers le dossier contenant les ' + task + '.\n')
        else:
            folder = os.path.join(save_path, task)
            
        converters = {'Date de debut de diffusion' : str, 'Date de fin de diffusion' : str, 'Lien' : str, "Numero d'ordre" : str, "Numero d'episode" : str, "Type d'enregistrement" : str, 'nombre de passage' : str, "ayants_droit": str}
        
        df_lst = list()
        for directory in tqdm(os.listdir(folder), desc = "dossiers"):
            path = os.path.join(folder, directory)
            if os.path.isdir(path):
                for csv in tqdm(os.listdir(path), desc = "fichiers"):
                    print(csv)
                    if csv.endswith(".csv"):
                        df_lst.append(pd.read_csv(os.path.join(path, csv), sep = ';', index_col = False, converters = converters))
                        
        
        df = pd.concat(df_lst, sort=False)
        save_path = input('\nChoisissez un dossier pour sauvegarder le fichier concaténé.\n')
        if not save_path:
            return
        
        df = df.drop_duplicates()

        if task == 'oeuvres':
            df['TROU'] = df["Titre 1"].apply(trou)

            # Drop columns
            cols = [c for c in df.columns if c.lower()[:13] != 'ayants_droit.']
            df=df[cols]
            # Drop duplicates
            df.drop_duplicates(keep=False, inplace=True)


            df.to_csv(os.path.join(save_path, "GRILLE_PROG.csv"), sep = ';', index = False)
            print('"GRILLE_PROG.csv" a été créé dans ' + save_path + '.')
            
        else:
            df.to_csv(os.path.join(save_path, task + "_concaténés.csv"), sep = ';', index = False)
            print(task + '_concaténés.csv" a été créé dans ' + save_path + '.')
        


# In[3]:


def contenu(x):
    type_de_contenant = x[0]
    type_de_contenant_du_parent = x[1]

    if (type(type_de_contenant) == float or type_de_contenant == 'nan') and (type_de_contenant_du_parent != 'nan' or type(type_de_contenant_du_parent) != float):
        if type_de_contenant_du_parent == 'contenant de niveau 1':
            return 'contenu de niveau 1'
        elif type_de_contenant_du_parent == 'contenant de niveau 2':
            return 'contenu de niveau 2'
    elif (type(type_de_contenant) == float or type_de_contenant == 'nan') and (type_de_contenant_du_parent == 'nan' or type(type_de_contenant_du_parent) == float):
        return "----TROU----"
    elif type(type_de_contenant) != float or type_de_contenant != 'nan':
        return type_de_contenant
    else:
        return "----TROU----"

    
def check_content(df):
    print("Check content will start")
    # Type de contenant
    contenant_start = time.time()
    df['Numero d\'ordre'].loc[df['Numero d\'ordre'] == ''] = np.nan
    df['Lien'].loc[df['Lien'] == ''] = np.nan
    df = df.astype({'Numero d\'ordre': float, 'Lien': float})
    # df['contenant tmp'] = False
    # df['contenant tmp'].loc[(df['Numero d\'ordre'].isin(df['Lien'])) & (df['Fichier source'].isin(df['Fichier source']))] = True
    parents = df.loc[(df['Fichier source'].isin(df['Fichier source'])) & (df['Lien'].isin(df['Numero d\'ordre']))].copy()
    parents['contenant tmp'] = True
    parents = parents[['Fichier source', 'Lien', 'contenant tmp']]
    parents = parents.dropna()
    df = df.merge(parents, left_on=['Fichier source', 'Numero d\'ordre'], right_on=['Fichier source', 'Lien'], how='left')


    print("contenant tmp : OK")
    df = df.rename({'Lien_x': 'Lien'},axis='columns')
    df = df.drop('Lien_y', axis=1)

    df['Type de contenant'] = np.nan
    df['Type de contenant'].loc[(df['contenant tmp'] == True) & (df['Lien'] != 0) & (df['Lien'].isnull() == False) & (df['Lien'] != '') & (df['Lien'] is not None)] = "contenant de niveau 2"
    df['Type de contenant'].loc[(df['contenant tmp'] == True) & (df['Lien'] == 0)] = "contenant de niveau 1"
    df['Type de contenant'].loc[(df['contenant tmp'] != True) & (df['Lien'] == 0)] = "contenant isole"
    print("Type de contenant : OK")
    print("contenant time : ---- %s seconds ----" % (time.time() - contenant_start))


    # Type de contenu
    content_start = time.time()
    parent_merge_start = time.time()
    parents = df.loc[(df['Fichier source'].isin(df['Fichier source'])) & (df['Numero d\'ordre'].isin(df['Lien']))].copy()
    parents = parents.drop(['Code Declarant', 'Date de debut de diffusion', 'Date de fin de diffusion', 'Heure de debut de diffusion', 'Heure de fin de diffusion', 'Type d\'enregistrement', 'Type de titre 1', 'Titre 1', 'Type de titre 2', 'Titre 2', 'Numero de l\'episode', 'Genre de diffusion de l\'oeuvre', 'code genre', 'Duree de diffusion', 'Doublage et/ou sous-titrage', 'Lien', 'Nombre de passage', 'contenant tmp'], axis=1)
    parents = parents.rename({'Type de contenant': 'Type de contenant du parent'},axis='columns')
    df = df.merge(parents, left_on=['Fichier source', 'Lien'], right_on=['Fichier source', 'Numero d\'ordre'], how='left')
    print("parent merge time : ---- %s seconds ----" % (time.time() - parent_merge_start))

    contenu_start = time.time()
    df['item'] = [item for item in zip(df['Type de contenant'], df['Type de contenant du parent'])]
    df['Type de contenant'] = df['item'].apply(contenu)
    print("Contenu : OK")
    print("contenu time : ---- %s seconds ----" % (time.time() - contenu_start))
    print("content time : ---- %s seconds ----" % (time.time() - content_start))
    
    df = df.drop(['contenant tmp', 'Numero d\'ordre_y', 'Type de contenant du parent', 'Duree en secondes_y'], axis=1)
    df = df.rename({'Numero d\'ordre_x': 'Numero d\'ordre'})
    print("Check content: ok")
    return df


# In[4]:


def concat_tmp(task, save_path):
#Lit des fichiers tmp et les concatène en un    
    
    #Enregistre une liste qui contient les chemins "path/dossier" sous forme de string pour chaque dossier present dans path
    path_list = [os.path.join("tmp", task, folder) for folder in os.listdir(os.path.join("tmp", task)) if os.path.isdir(os.path.join("tmp", task, folder))]

    #Enregistre dans une liste tous les fichiers ayant l'extension ".tmp" presents dans path_list
    filename_lists = [[os.path.join(path, filename) for filename in os.listdir(path) if filename.endswith('.csv')] for path in path_list]
    
    del path_list
    
    #Concatenation des fichiers tmp
    if task == "oeuvres":
        sort_cols = ["Code Declarant", "Date de debut de diffusion", 'Heure de debut de diffusion', "Type d'enregistrement", "Lien", 'Heure de fin de diffusion']
        converters = {'Date de debut de diffusion' : str, 'Date de fin de diffusion' : str, 'Lien' : str, "Numero d'ordre" : str, "Numero de l'episode" : str, "Type d'enregistrement" : str, 'Nombre de passage' : str}
    else:
        sort_cols = ["Code Declarant", "Date de debut de diffusion", 'Heure de debut de diffusion', "Type d'enregistrement"]
        converters = {'Date de debut de diffusion' : str, "Numero d'ordre": str}
    
   #Sauvegarde sur le pc les nouveaux fichiers issues de la concatenation 
    
    for filenames in tqdm(filename_lists, desc = "Concatenation des " + task + " par chaîne"):
        df_list = [pd.read_csv(filename, index_col = False, sep = ';', converters = converters) for filename in filenames]
        df = pd.concat(df_list).sort_values(sort_cols).reset_index(drop = True)
        del df_list
        
        #Créé le nouveau fichier
        df = df.dropna(subset = ['Date de debut de diffusion', 'Heure de debut de diffusion'])
        new_path = os.path.join(save_path, task, str(df['Code Declarant'].values[0]).strip(), "_".join([str(df['Code Declarant'].values[0]).strip(), str(df['Date de debut de diffusion'].values[0])[:4], str(df['Date de debut de diffusion'].values[-1])[:4]]) + ".csv")
        create_dir(os.path.join(save_path, task, str(df['Code Declarant'].values[0]).strip()))
        if task == 'oeuvres':
            df = check_content(df)
        df.to_csv(new_path, index = False, sep = ';')
        print(new_path)

        del df
            
        
    del filename_lists


# In[5]:


def create_dir(path):

    if not os.path.isdir(path):
        os.makedirs(path)


# In[6]:


def get_hole_begin_hour(item):
#Replace dans l'ordre l'heure de debut de diffusion pour les trous et les oeuvres
    
    """Si il n'y a pas d'heure de debut de diffusion il s'agit d'un trou.
    L'heure de debut est alors egal à l'heure de fin du programme precedent"""
    
    hole_hour = item[0]
    begin_hour = item[5]
    titre = item[4]

    if  titre == "----TROU----":
        return hole_hour
    
    else:
        return begin_hour
# In[8]:


def get_hole_end_hour(item):
    
    end_hour = item[0]
    hole_hour = item[1]
    titre = item[4]

    if titre == "----TROU----":
        return hole_hour
    
    else:
        return end_hour
    
    
def get_date(item):
    
    begin_date = item[2]
    end_date = item[3]
    titre = item[4]
    end_date_before = item[6]
    
    if titre == "----TROU----":
        return end_date_before
    
    else:
        return begin_date


def get_end_date(item):
    
    titre = item[4]
    end_date_current = item[7]
    begin_hour = item[0]
    end_hour = item[1]
    date_begin = item[6]
    
    if titre == "----TROU----":
        return end_date(begin_hour, end_hour, date_begin)
    
    else:
        return end_date_current

# In[7]:


def get_holes(df):
    
    #Enregistre la duree des trous dans une liste 
    df['Trou'] = df['item'].apply(substract_broadcast_time)
    
    #Selectionne les trous et vides les celulles
    holes = df[(df['Trou'] != '00:00:00') & (df['code genre'] != 'PUB')].copy()
    other_cols = ["Numero d'ordre", "Heure de fin de diffusion", "Type d'enregistrement", "Type de titre 1", "Titre 1", "Type de titre 2", "Titre 2", "Numero de l'episode", "Genre de diffusion de l'oeuvre",  "Duree de diffusion", "Date de fin de diffusion", "Doublage et/ou sous-titrage", "Lien", "Nombre de passage"]
    holes[other_cols] = np.nan
    df['Trou'] = ''

    
    #Concatène le tableau de trou avec celui des programmes
    df2 = pd.concat([df, holes]).dropna(subset = ['Trou']).sort_index()
    del df, holes
    #Enregistre les durees de diffusion des programmes et des trous dans une liste
    duree = df2["Duree de diffusion"].tolist()
    trou = df2["Trou"].tolist()
    
    #si enregistre la duree du programme. S'il n'y a pas de programme enregistre la duree du trou
    duree_trou = [x if str(x) != "nan" and len(x) > 0 else y for (x, y) in zip(duree, trou)]
    df2['Duree de diffusion'] = duree_trou
    
    #S'il la cellule de la colonne "Titre 1" est vide, ecrire "----TROU----" à l'interieur
    df2['Titre 1'] = ["----TROU----" if str(string) == "nan" else string for string in df2['Titre 1']]
    df2 = df2.sort_values(["Code Declarant", 'Date de debut de diffusion', 'Heure de debut de diffusion', "Trou"])
    
    #Ajouter les heures de debut et de fin des trous
    df2['item'] = [item for item in zip(df2['Heure de fin de diffusion'], df2['Heure de debut de diffusion'].shift(-1), df2['Date de debut de diffusion'], df2['Date de debut de diffusion'].shift(-1), df2['Titre 1'], df2['Heure de debut de diffusion'])]
    
    df2["Heure de fin de diffusion"] = df2['item'].apply(get_hole_end_hour)
    
    df2['item'] = [item for item in zip(df2['Heure de fin de diffusion'].shift(1), df2['Heure de debut de diffusion'].shift(-1), df2['Date de debut de diffusion'], df2['Date de debut de diffusion'].shift(-1), df2['Titre 1'], df2['Heure de debut de diffusion'], df2['Date de fin de diffusion'].shift(1), df2['Date de fin de diffusion'])]
    
    df2['Heure de debut de diffusion'] = df2['item'].apply(get_hole_begin_hour)
    df2['Date de debut de diffusion'] = df2['item'].apply(get_date)
    df2['Date de fin de diffusion'] = df2['item'].apply(get_end_date)
    
     #trier les lignes du tableau pour replacer les trous entre les programmes
    df2 = df2.sort_values(["Code Declarant", "Date de debut de diffusion", 'Heure de debut de diffusion']).reset_index(drop = True)


    

    return df2


# In[8]:


def substract_broadcast_time(item):
    """Verifie la presence de temps non declare entre deux programmes. 
    Si oui, renvoie la difference entre l'heure de debut du programme et l'heure de fin du programme precedent.
    Si non, renvoie "00:00:00" """
    
    begin = item[0]
    end = item[1]
    
    if begin is not None and end > begin:
        td = end - begin
            
    else:
        return "00:00:00"

    td = int(td)
    substract = ":".join([str(td // 3600).rjust(2, '0'), str(td // 60 % 60).rjust(2, '0'), str(td % 60).rjust(2, '0')])

    return substract


def str_to_td(string):
#Convetie une string au format "00:00:00" en secondes
    
    if type(string) == float or string == '':
        return np.nan
    try:
        td = timedelta(hours = int(string[0:2]), minutes = int(string[3:5]), seconds = int(string[6:]))
        
    except ValueError:
        return np.nan
    
    return td.seconds


def td_to_str(td):

    if str(td) == "nan":
        return td

    seconds = int(td)

    string = ":".join([str(seconds // 3600).rjust(2, '0'), str(seconds // 60 % 60).rjust(2, '0'), str(seconds % 60).rjust(2, '0')])

    return string

def end_date(begin, end, date):
    
    try:
        begin = timedelta(hours = int(begin[0:2]), minutes = int(begin[3:5]), seconds = int(begin[6:]))
        end = timedelta(hours = int(end[0:2]), minutes = int(end[3:5]), seconds = int(end[6:]))
        if begin < end :
            return date
        else:
            new_date = datetime.strptime(date, "%Y%m%d") + timedelta(days = 1)
            return new_date.strftime("%Y%m%d")
    except ValueError:
        return date
    
def broadcast_end(begin, duration):
    #Additionne l'heure de debut d'un programme avec sa duree pour obtenir l'heure de fin     
    
    try:
        begin_td = timedelta(hours = int(begin[0:2]), minutes = int(begin[3:5]), seconds = int(begin[6:]))
        end_td = timedelta(hours = int(duration[0:2]), minutes = int(duration[3:5]), seconds = int(duration[6:]))
        td = begin_td + end_td
        
    except ValueError:
        return begin
    
    seconds = int(td.seconds)

    end = ":".join([str(seconds // 3600).rjust(2, '0'), str(seconds // 60 % 60).rjust(2, '0'), str(seconds % 60).rjust(2, '0')])
    return end


def get_datetime(item):

    begin_hour = item[0]
    end_hour = item[1]
    begin_date = item[2]
    end_date = item[3]
    
    if str(end_hour) == "nan" or end_hour == '' or str(begin_hour) == "nan" or begin_hour == '' or str(begin_date) == "nan" or begin_date == '' or str(end_date) == "nan" or end_date == '':
        return (None, None)
    
    begin = datetime.strptime(begin_date + begin_hour, "%Y%m%d%H:%M:%S")
    end = datetime.strptime(end_date + end_hour, "%Y%m%d%H:%M:%S")
    
    return (begin.timestamp(), end.timestamp())


def get_all_datetime(item):

    begin_hour = item[0]
    prev_begin_hour = item[1]
    next_begin_hour = item[2]
    
    end_hour = item[3]
    prev_end_hour = item[4]
    next_end_hour = item[5]
    
    begin_date = item[6]
    prev_begin_date = item[7]
    next_begin_date = item[8]
    
    end_date = item[9]
    prev_end_date = item[10]
    next_end_date = item[11]
    
    title = item[12]
    
    if str(begin_hour) == "nan" or begin_hour == '' or str(begin_date) == "nan" or begin_date == '':
        begin = np.nan
    else:
        begin = datetime.strptime(begin_date + begin_hour, "%Y%m%d%H:%M:%S")
        
    if str(prev_begin_hour) == "nan" or prev_begin_hour == '' or str(prev_begin_date) == "nan" or prev_begin_date == '':
        prev_begin = np.nan
    else:
        prev_begin = datetime.strptime(prev_begin_date + prev_begin_hour, "%Y%m%d%H:%M:%S")
        
    if str(next_begin_hour) == "nan" or next_begin_hour == '' or str(next_begin_date) == "nan" or next_begin_date == '':
        next_begin = np.nan
    else:
        next_begin = datetime.strptime(next_begin_date + next_begin_hour, "%Y%m%d%H:%M:%S")
        
        
    if str(end_hour) == "nan" or end_hour == '' or str(end_date) == "nan" or end_date == '':
        end = np.nan
    else:    
        end = datetime.strptime(end_date + end_hour, "%Y%m%d%H:%M:%S")
        
    if str(prev_end_hour) == "nan" or prev_end_hour == '' or str(prev_end_date) == "nan" or prev_end_date == '':
        prev_end = np.nan
    else:    
        prev_end = datetime.strptime(prev_end_date + prev_end_hour, "%Y%m%d%H:%M:%S")
        
    if str(next_end_hour) == "nan" or next_end_hour == '' or str(next_end_date) == "nan" or next_end_date == '':
        next_end = np.nan
    else:    
        next_end = datetime.strptime(next_end_date + next_end_hour, "%Y%m%d%H:%M:%S")
    
    return (begin, prev_begin, next_begin, end, prev_end, next_end, title)


# In[9]:


def get_genre(genre):
    
    genre_dict = {
            "ANN" : "Annonce",
            "ANT" : "Anthologie",
            "BA " : "Bande annonce",
            "B.A" : "Bande annonce",
            "BAC" : "Bande annonce chaîne",
            "BAF" : "Bande annonce film",
            "BAL" : "Ballet",
            "CHR" : "Chronique",
            "CHT" : "Variete chantee",
            "CLI" : "Clip",
            "CMA" : "Court metrage d'animation",
            "CMF" : "Court metrage de fiction",
            "CMD" : "Court metrage documentaire",
            "CME" : "Court metrage",
            "COM" : "Commentaire",
            "DAN" : "Dessin anime",
            "DBA" : "Debat",
            "DEB" : "Debut de journee",
            "DES" : "Desannonce",
            "DMU" : "Documentaire musical",
            "DOC" : "Documentaire",
            "DRA" : "Dramatique",
            "EMI" : "Emission",
            "EMS" : "Suite de l'emission",
            "ENT" : "Entretien",
            "ESS" : "Essai",
            "EXT" : "Extrait",
            "FDS" : "Fond sonore",
            "FEU" : "Feuilleton",
            "FIL" : "Film",
            "FIN" : "Fin de journee",
            "GEN" : "Generique",
            "GRA" : "Oeuvre graphique",
            "ILL" : "Musique d'illustration sonore",
            "IMA" : "Image",
            "IND" : "Indicatif",
            "JAZ" : "Jazz",
            "JIN" : "Jingle",
            "LEC" : "Lecture",
            "LYR" : "Oeuvre lyrique",
            "MAG" : "Magazine",
            "MIR" : "Mire",
            "MOR" : "Musique originale film",
            "NWS" : "Informations",
            "ORC" : "Variete instrumentale",
            "PAN" : "Panne",
            "PAR" : "Parodie",
            "POE" : "Poème",
            "PRO" : "Oeuvre litteraire",
            "PUB" : "Publicite",
            "REC" : "Recit",
            "REP" : "Reportage",
            "SPC" : "Sponsoring",
            "SPO" : "Musique de sponsoring",
            "SYM" : "Musique symphonique",
            "SKE" : "Sketch",
            "TEL" : "Telefilm",
            "THE" : "Theâtre",
            "TEX" : "Texte",
            "TP " : "Texte de presentation",
            "T.P" : "Texte de presentation",
            "VIA" : "Video d'art"
            }

    if genre in genre_dict.keys():
        return (genre_dict[genre], genre)
    return (genre, genre)

def get_ayant_droit(dip4_dict, ayant_droit):
    
    ayants_droits_dict = {
            "AC " : "Acteur",
            "A.C" : "Acteur",
            "DS " : "Doubleur-Sous titreur",
            "D.S" : "Doubleur-Sous titreur",
            "PH " : "Photographe",
            "P.H" : "Photographe",
            "ADA" : "Adaptateur",
            "AD " : "Adaptateur",
            "A.D" : "Adaptateur",
            "ARR" : "Arrangeur",
            "A.R" : "Arrangeur",
            "AR " : "Arrangeur",
            "PRO" : "Producteur",
            "A  " : "Auteur",
            "AUT" : "Auteur",
            "REA" : "Realisateur",
            "RE " : "Realisateur",
            "R.E" : "Realisateur",
            "C  " : "Compositeur",
            "COM" : "Compositeur",
            "EDI" : "Editeur",
            "E  " : "Editeur",
            "INT" : "Interprète",
            "IN " : "Interprète",
            "I.N" : "Interprète",
            "ACT" : "Acteur",
            "TRA" : "Traducteur",
            "TR " : "Traducteur",
            "T.R" : "Traducteur",
            "SCE" : "Scenariste",
            "CHO" : "Choregraphe",
            "PHO" : "Photographe",
            "PEI" : "Peintre",
            "SCU" : "Sculpteur",
            "C.A" : "Auteur-Compositeur",
            "CA " : "Compositeur-Auteur",
            "MEC" : "Metteur en scène",
            "MS " : "Metteur en scène",
            "M.S" : "Metteur en scène",
            "A.T" : "Architecte",
            "AT " : "Architecte",
            "C.D" : "Costumier",
            "CD " : "Costumier",
            "DIA" : "Dialoguiste",
            "A.S" : "Dialoguiste",
            "AS " : "Dialoguiste",
            "D.G" : "Designer",
            "DG " : "Designer",
            "D.W" : "Dessinateur",
            "DW " : "Dessinateur",
            "F.A" : "Artiste",
            "FA " : "Artiste",
            "DIS" : "Distributeur",
            "FD " : "Distributeur",
            "F.D" : "Distributeur",
            "EOP" : "Editeur de l'oeuvre litteraire preexistante",
            "GRA" : "Graphiste",
            "AG " : "Graphiste",
            "A.G" : "Graphiste",
            "C.P" : "Infographiste",
            "CP " : "Infographiste",
            "ITW" : "Interviewer",
            "ITX" : "Interviewe",
            "S.A" : "Sous Arrangeur",
            "SA " : "Sous Arrangeur",
            "S.E" : "Sous Editeur",
            "SE " : "Sous Editeur",
            "UNK" : "Role de l'ayant droit inconnu",
            "UN " : "Role de l'ayant droit inconnu",
            "U.N" : "Role de l'ayant droit inconnu"
            }
    
    if ayant_droit[0] in ayants_droits_dict.keys():
        role = ayants_droits_dict[ayant_droit[0]]
    else:
        role = ayant_droit[0]
        
    first_name = ayant_droit[1]
    name = ayant_droit[2]
    code = ayant_droit[3]
    
    return {'Code Declarant' : dip4_dict['Code Declarant'], 
            "Numero d'ordre" : dip4_dict["Numero d'ordre"], 
            'Date de debut de diffusion' : dip4_dict['Date de debut de diffusion'], 
            'Heure de debut de diffusion' : dip4_dict['Heure de debut de diffusion'],
            "Type d'enregistrement" : dip4_dict["Type d'enregistrement"],
            "Role" : role,
            "Nom" : first_name,
            "Prenom" : name}


# In[10]:


def get_oeuvres(line, dip4_dict):
    
    #Titres et Oeuvres
    dip4_dict["Type de titre 1"] = line[30:32]
    dip4_dict["Titre 1"] = line[33:92].strip()
    dip4_dict["Type de titre 2"] = line[93:95]
    dip4_dict["Titre 2"] = line[96:155]
    dip4_dict["Numero de l'episode"] = line[159:164]
    genre = get_genre(line[174:177])
    dip4_dict["Genre de diffusion de l'oeuvre"] = genre[0]
    dip4_dict["code genre"] = genre[1]
    dip4_dict["Duree de diffusion"] = ":".join([line[177:179], line[179:181], line[181:183]])
    if len(line) < 197 :
        dip4_dict["Doublage et/ou sous-titrage"] = np.nan
        dip4_dict["Lien"] = np.nan
        dip4_dict["Nombre de passage"] = np.nan
        dip4_dict["Heure de fin de diffusion"] = broadcast_end(dip4_dict['Heure de debut de diffusion'], dip4_dict["Duree de diffusion"])
        dip4_dict["Date de fin de diffusion"] = end_date(dip4_dict['Heure de debut de diffusion'], dip4_dict["Heure de fin de diffusion"], dip4_dict['Date de debut de diffusion'])
        return dip4_dict

    dip4_dict["Doublage et/ou sous-titrage"] = line[197]
    dip4_dict["Lien"] = line[217:223]
    dip4_dict["Nombre de passage"] = line[225:229]
    dip4_dict["Heure de fin de diffusion"] = broadcast_end(dip4_dict['Heure de debut de diffusion'], dip4_dict["Duree de diffusion"])
    dip4_dict["Date de fin de diffusion"] = end_date(dip4_dict['Heure de debut de diffusion'], dip4_dict["Heure de fin de diffusion"], dip4_dict['Date de debut de diffusion'])
                
    return dip4_dict


# In[11]:


def dip4_to_dict(line, tasks):
    
    dip4_dict = dict()
    
    #save the date, TV channel and number of TV program
    dip4_dict['Code Declarant'] = line[:3]
    dip4_dict["Numero d'ordre"] = line[3:9]
    if line[17:19].isdigit() and int(line[17:19]) > 23:
        
        try:
            date = datetime.strptime(line[9:17], "%Y%m%d") + timedelta(days = 1)
            dip4_dict['Date de debut de diffusion'] = date.strftime("%Y%m%d")
        except:
            dip4_dict['Date de debut de diffusion'] = line[9:17]
            
        dip4_dict['Heure de debut de diffusion'] = ":".join(["0" + str(int(line[17:19]) - 24), line[19:21], line[21:23]])
        
    else:
        dip4_dict['Date de debut de diffusion'] = line[9:17]
        dip4_dict['Heure de debut de diffusion'] = ":".join([line[17:19], line[19:21], line[21:23]])
        
    dip4_dict["Type d'enregistrement"] = line[25:27]
    
    if 'oeuvres' in tasks and dip4_dict["Type d'enregistrement"] == "10":
        dip4_dict = get_oeuvres(line, dip4_dict)
    
    if 'ayants_droit' in tasks and dip4_dict["Type d'enregistrement"] == "20" :
        ayants_droit_list = [
            [line[30:33], line[33:63].rstrip(), line[63:89].rstrip(), line[89:104]],
            [line[110:113], line[113:143].rstrip(), line[143:169].rstrip(), line[169:184]],
            [line[190:193], line[193:223].rstrip(), line[223:249].rstrip(), line[249:264]],
            [line[270:273], line[273:303].rstrip(), line[303:329].rstrip(), line[329:344]]
            ]
        dict_list = [get_ayant_droit(dip4_dict, ayant_droit) for ayant_droit in ayants_droit_list if ayant_droit[0].strip() != '']

    else:
        dict_list = [dip4_dict]
        
    return (dip4_dict, dict_list)


# In[12]:


def file_to_dataframe(file_path, tasks):
    
    #open file
    with open(file_path, 'r', encoding = 'latin-1') as file :
            dip4 = file.read()
    
    #split lines into a list
    dip4_lines_list = [line for line in dip4.splitlines() if len(line) > 1]  

    #transfrom the list of lines into a list of dictionnaries
    dip4_list = [dip4_to_dict(line, tasks) for line in dip4_lines_list[1 : -2]]
    
    #make a dataframe from the list of dictionnaries
    if 'ayants_droit' in tasks:
        new_list = [dic for dip4_dict, dict_list in dip4_list for dic in dict_list]
        df_ayants = pd.DataFrame(new_list)
        del new_list
        df_ayants['Fichier source'] = file_path[file_path.rfind('/') + 1:]
        df_ayants = df_ayants[df_ayants["Type d'enregistrement"] == "20"].reset_index(drop = True)
    else:
        df_ayants = None
        
    if 'oeuvres' in tasks:
        dict_list = [dip4_dict for dip4_dict, dict_list in dip4_list]
        df_oeuvres = pd.DataFrame(dict_list)
        del dict_list
        df_oeuvres['Fichier source'] = file_path[file_path.rfind('/') + 1:]
        df_oeuvres['ayants_droit'] = False
        df_oeuvres['ayants_droit'].loc[(df_oeuvres["Numero d'ordre"].isin(df_ayants["Numero d'ordre"])) & (df_oeuvres['Fichier source'].isin(df_ayants['Fichier source']))] = True
        df_princ = df_oeuvres[df_oeuvres['Lien'] == '000000'].reset_index(drop = True).copy()
        df_sec = df_oeuvres[df_oeuvres["Type d'enregistrement"] == '10'][df_oeuvres['Lien'] != '000000'].reset_index(drop = True).copy()
    else:
        df_princ = df_sec = None

#     ###########################################################################################
#     #                                           optimiser le code
    
#     # Checking if we have an 'ayant droit' for each 'oeuvre' when 'Lien' == 0
#     AD = False
#     AD_list = []
#     ayant_list = list(df_ayants["Numero d'ordre"])
#     for no in df_princ["Numero d'ordre"]:
#         if no in ayant_list:
#             AD_list.append(True)
#         else:
#             AD_list.append(False)
#     df_princ['ayants_droit'] = AD_list
    
#     # Checking if we have an 'ayant droit' for each 'oeuvre' when 'Lien' != 0
#     AD = False
#     AD_list = []
#     ayant_list = list(df_ayants["Numero d'ordre"])
#     for no in df_sec["Numero d'ordre"]:
#         if no in ayant_list:
#             AD_list.append(True)
#         else:
#             AD_list.append(False)
#     df_sec['ayants_droit'] = AD_list

#     df_princ['ayants_droit'] = False
#     df_princ['ayants_droit'].loc[(df_princ["Numero d'ordre"].isin(df_ayants["Numero d'ordre"]))] = True
#     df_sec['ayants_droit'] = False
#     df_sec['ayants_droit'].loc[(df_sec["Numero d'ordre"].isin(df_ayants["Numero d'ordre"]))] = True



    del dip4_list 
    return (df_princ, df_sec, df_ayants)


# In[13]:


def create_tmp_files(file_path_list, tasks, oeuvres_cols, ayants_droit_cols):
    
    #turn the files into dataframes and save them in a list
    df_list = [file_to_dataframe(file_path, tasks) for file_path in file_path_list]

    #concat the dataframes
    if 'oeuvres' in tasks:
        df_princ_list = [df_princ for df_princ, df_sec, df_ayants in df_list]
        df_sec_list = [df_sec for df_princ, df_sec, df_ayants in df_list]
        
        df_princ = pd.concat(df_princ_list)
        df_sec = pd.concat(df_sec_list)

        del df_princ_list, df_sec_list
   
    if 'ayants_droit' in tasks:
        df_ayants_list = [df_ayants for df_princ, df_sec, df_ayants in df_list]
        df_ayants = pd.concat(df_ayants_list)
            
        del df_ayants_list
            
    #add holes
    if "oeuvres" in tasks:
        df_princ = df_princ.sort_values(['Code Declarant', 'Date de debut de diffusion', 'Heure de debut de diffusion'])
        df_princ['item'] = [item for item in zip(df_princ['Heure de fin de diffusion'], df_princ['Heure de debut de diffusion'].shift(-1), df_princ['Date de fin de diffusion'], df_princ['Date de debut de diffusion'].shift(-1))]
        df_princ['item'] = df_princ['item'].apply(get_datetime)
        df_princ.head(10)
        df_princ = get_holes(df_princ)
        df_princ['Duree en secondes'] = df_princ['Duree de diffusion'].apply(str_to_td)
            
        df_princ['item'] = [item for item in zip(df_princ['Heure de debut de diffusion'], df_princ['Heure de debut de diffusion'].shift(-1), df_princ['Heure de debut de diffusion'].shift(1),                                                 df_princ['Heure de fin de diffusion'], df_princ['Heure de fin de diffusion'].shift(-1), df_princ['Heure de fin de diffusion'].shift(1),                                                 df_princ['Date de debut de diffusion'], df_princ['Date de debut de diffusion'].shift(-1), df_princ['Date de debut de diffusion'].shift(1),                                                 df_princ['Date de fin de diffusion'], df_princ['Date de fin de diffusion'].shift(-1), df_princ['Date de fin de diffusion'].shift(1),                                                 df_princ['Titre 1'])]
        df_princ['item'] = df_princ['item'].apply(get_all_datetime)       

        df_princ = df_princ.drop(['item'], axis = 1)
        
        df_sec['Lien'] = df_sec['Lien'].apply(str)     
        df_sec['Duree en secondes'] = df_sec['Duree de diffusion'].apply(str_to_td)
                    
        #Trier les colonnes du plus ancien vers le plus récent
        sort_oeuvres_cols = ["Date de debut de diffusion", 'Heure de debut de diffusion', "Type d'enregistrement", "Lien", 'Heure de fin de diffusion']
        df_oeuvres = pd.concat([df_princ, df_sec])
#         oeuvres_cols.append('ayants_droit')
#         oeuvres_cols.append('Type de contenant')
        df_oeuvres['Type de contenant'] = np.nan
        del df_princ, df_sec
        df_oeuvres = df_oeuvres.sort_values(sort_oeuvres_cols).reset_index(drop = True)[oeuvres_cols]

        global last_contenant
        last_contenant = 1
        
        #create a folder for the tmp file if it doesn't exist
        path = os.path.join("tmp", "oeuvres", df_oeuvres['Code Declarant'].values[0].strip())
        create_dir(path)

        #create a tmp file
        df_oeuvres.to_csv(os.path.join(path,                           "_".join([df_oeuvres['Code Declarant'].values[0].strip(), str(df_oeuvres['Date de debut de diffusion'].values[0])[:4]])                           + ".csv"), sep = ';', index = False)

        
    if 'ayants_droit' in tasks and "Role" in df_ayants.columns and "Prenom" in df_ayants.columns and "Nom" in df_ayants.columns :
        #Trier les colonnes du plus ancien vers le plus récent
        sort_ayants_cols = ["Date de debut de diffusion", 'Heure de debut de diffusion', "Type d'enregistrement"]
        df_ayants = df_ayants.sort_values(sort_ayants_cols).reset_index(drop = True)[ayants_droit_cols]
        
        #create a folder for the tmp file if it doesn't exist
        path = os.path.join("tmp", "ayants_droit", df_ayants['Code Declarant'].values[0].strip())
        create_dir(path)        
        
        #create a tmp file
        df_ayants.to_csv(os.path.join(path,                           "_".join([df_ayants['Code Declarant'].values[0].strip(), str(df_ayants['Date de debut de diffusion'].values[0])[:4]])                           + ".csv"), sep = ';', index = False)
                
        del df_ayants
        df_oeuvres


# In[14]:


def list_channel(item, split_path, prev_channel, file_path_list, oeuvres_cols, ayants_droit_cols, tasks):
    
    if "cable_satellite" in split_path:
        if "fait" not in split_path and "faits" not in split_path and "Fait" not in split_path and "Faits" not in split_path:
            return prev_channel, file_path_list

    channel = item[1]        

    if prev_channel is not None and prev_channel != channel and file_path_list:       

        create_tmp_files(file_path_list, tasks, oeuvres_cols, ayants_droit_cols)
        del file_path_list
        file_path_list = list()

    file_path_list.append(item[0])
    prev_channel = channel
                
    return prev_channel, file_path_list


# In[15]:


def browse_dip4(df, tasks, oeuvres_cols, ayants_droit_cols):
    
    file_path_list = list()
    prev_channel = None
    
    items = zip(df['Chemin complet'], df['Code Declarant'])
    length = len(df)
    
    for item in tqdm(items, desc = ", ".join(tasks), total = length):
        split_path = item[0].split("/")
    
        prev_channel, file_path_list = list_channel(item, split_path, prev_channel, file_path_list, oeuvres_cols, ayants_droit_cols, tasks)
                   
    if len(file_path_list) > 0:
        create_tmp_files(file_path_list, tasks, oeuvres_cols, ayants_droit_cols)
        del file_path_list


# In[16]:


def get_tasks(var_1, var_2):
    if var_1 + var_2 == 2:
        folder_1 = 'oeuvres'
        folder_2 = 'ayants_droit'
        tasks = ['oeuvres', 'ayants_droit']
        
    elif var_1 == 1:
        folder_1 = 'oeuvres'
        folder_2 = ''
        tasks = ['oeuvres']
        
    elif var_2 == 1:
        folder_1 = ''
        folder_2 = 'ayants_droit'
        tasks = ['ayants_droit']
    else:
        return ''

    return tasks, folder_1, folder_2


# In[17]:


def parse_dip4(df):

    if df is None: 
        
        dip4_list = input('Donnez le chemin de la liste des fichiers DIP4 à employer pour commencer la conversion.\n')

        if not dip4_list:
            return
        
        if not os.path.isfile(dip4_list):
            raise Exception('"' + dip4_list + '" not found. May be the path is not correct.')
            
        try:
            df = pd.read_csv(dip4_list, index_col = False, sep = ';', encoding = 'latin-1')
        except:
            print("La liste des fichiers DIP4 chargée n'est pas valide")
            return
        
    cols = ['Lien', 'Date de debut de diffusion', 'Duree de diffusion', 'Longueur des lignes', 'Heure de diffusion', 'Nom du fichier', 'Chemin complet', 'Code Declarant']
    for col in cols:
        if col not in df.columns:
            print("La liste des fichiers DIP4 chargée n'est pas valide")
            return
        
    df_lst = [df[~df['Lien'].isnull()], df[~df['Date de debut de diffusion'].isnull()], df[~df['Duree de diffusion'].isnull()], df[~df['Longueur des lignes'].isnull()], df[~df['Heure de diffusion'].isnull()]]
    
    df2 = pd.concat(df_lst).drop_duplicates()

    if len(df2) > 0:
        df2.to_csv("DIP4_exclus_de_l'analyse.csv", sep = ';')
    
    df = df[df['Lien'].isnull()]        [df['Date de debut de diffusion'].isnull()]        [df['Duree de diffusion'].isnull()]        [df['Longueur des lignes'].isnull()]        [df['Heure de diffusion'].isnull()]        .drop_duplicates('Nom du fichier')

    #save columns    
    oeuvre_cols = ["Fichier source", "Code Declarant", "Numero d'ordre", "Date de debut de diffusion", "Date de fin de diffusion", "Heure de debut de diffusion", "Heure de fin de diffusion", "Type d'enregistrement", "Type de titre 1", "Titre 1", "Type de titre 2", "Titre 2", "Numero de l'episode",            "Genre de diffusion de l'oeuvre", "code genre",  "Duree de diffusion", "Duree en secondes",            "Doublage et/ou sous-titrage", "Lien", "Nombre de passage", "Type de contenant"]
    
    ayants_droit_cols = ["Fichier source", "Code Declarant", "Numero d'ordre", "Date de debut de diffusion", "Heure de debut de diffusion", "Type d'enregistrement", "Role", "Nom", "Prenom"]

    save_path = input('Choisissez le chemin d\'un dossier pour sauvegarder les nouveaux fichiers créés.\n')

    if not save_path:
        return
    tasks_choice = input('Quels documents souhaitez-vous produire ? \n-"1" pour les oeuvres, \n-"2" pour les ayants droit,\n-"1" et "2" pour selectionner les deux choix.\n')
    if '1' in tasks_choice and '2' in tasks_choice:
        var_1 = 1
        var_2 = 1 
    elif '1' in tasks_choice:
        var_1 = 1
    elif '2' in tasks_choice:
        var_2 = 1
        
    tasks, folder_1, folder_2 = get_tasks(var_1, var_2)
        
    if not tasks:
        return
 
    if "oeuvres" not in tasks and "ayants_droit" not in tasks:
        return

    browse_dip4(df, tasks, oeuvre_cols, ayants_droit_cols)

    print("Création des fichiers temporaires terminés")
    
    for task in tasks:
        print("Concatenation des " + task)
        concat_tmp(task, save_path)

    if var_1 + var_2 == 2:
        print('"{}" et "{}" ont ete créé dans "{}"'.format(folder_1, folder_2, save_path))
        
    else:
        print('"{}{}" a ete créé dans "{}"'.format(folder_1, folder_2, save_path))

    if 'oeuvres' in tasks:
        concatenate_final_csv("oeuvres", save_path)

    if 'ayants_droit' in tasks:
        concatenate_final_csv('ayants_droit', save_path)
    
    shutil.rmtree("tmp")


# # 2. Find and Check dip4 to create "liste_dip4.csv" file

# In[18]:


def erase_percent(x):
    
    if type(x) != float and x != '':
        return x[:x.rfind(' ')]
    
    return x



def separate_percent(x):
    
    if type(x) != float and x != '':
        return x.split()[-1]
    
    return x



def anomalie_nombre(pronom, feature, string):
    
    if string.strip() == '':
        return "{} manquant ".format(feature).capitalize()
    else:
        return "{}{} n'est pas un nombre ".format(pronom, feature)



def get_percent(count, lenght):
    
    percent = count / lenght * 100
    
    ret = str(round(percent, 1)) + "%"

    if ret == "0.0%":
        return "0.1%"
 
    return ret


# In[19]:


def check_dip4(lines, code, chaine, nom, chemin):
    
    lenght = len(lines[1:-2])
    
    len_count = order_count = date_count = hour_count = type_count = title_count =    num_count = genre_count = duree_count = lien_count = passage_count = enr_count = 0
    
    dic = {
            "Nom de la chaîne" : chaine, "Code Declarant" : code, "Longueur des lignes" : np.nan,\
            "Numero d'ordre" : np.nan, "Date de debut de diffusion" : np.nan, "Heure de diffusion" : np.nan,\
            "Numero de l'episode" : np.nan, "Genre de diffusion de l'oeuvre" : np.nan, "Duree de diffusion" : np.nan,\
            "Lien" : np.nan, "Titre" : np.nan, "Nombre de passage" : np.nan, "Type de titre" : np.nan, "Nom du fichier": nom,\
            "Extension" : chemin[chemin.rfind('.'):], "Enregistrement 99": np.nan, "Chemin complet" : chemin\
          }
    
    normal_lenght = 228

    for line in lines[1:-2]:
        
        if len(line) < normal_lenght:
            len_count += 1
            dic["Longueur des lignes"] = "Ligne de declaration incomplète "            + get_percent(len_count, lenght)
            
        else:
            if not line[3:9].isdigit():
                order_count += 1
                dic["Numero d'ordre"] = anomalie_nombre("Le ", "numero d'ordre", line[3:9])                + get_percent(order_count, lenght)
                
                
            if not line[9:17].isdigit():
                date_count += 1
                dic["Date de debut de diffusion"] = anomalie_nombre("La ", "date", line[9:17])                + get_percent(date_count, lenght)
                                            
            if not line[17:23].isdigit():
                hour_count += 1
                dic["Heure de diffusion"] = anomalie_nombre("l'", "heure", line[17:23])                + get_percent(hour_count, lenght)
                
                
            if line[30:32].strip() == '':
                type_count += 1
                dic["Type de titre"] = "Type de titre manquant "                + get_percent(type_count, lenght)
                
                
            if line[33:92].strip() == '':
                title_count += 1
                dic["Titre"] = "Titre manquant " + get_percent(title_count, lenght)
                
                
            if not line[159:164].isdigit():
                num_count += 1
                dic["Numero de l'episode"] = anomalie_nombre("Le ", "numero de l'episode", line[159:164])                + get_percent(num_count, lenght)
                
                
            if line[174:177].strip() == '':
                genre_count += 1
                dic["Genre de diffusion de l'oeuvre"] = "Genre de l'oeuvre manquant "                + get_percent(genre_count, lenght)
                
            if not line[177:183].isdigit():
                duree_count += 1
                dic["Duree de diffusion"] = anomalie_nombre("La ", 'duree', line[177:183])                + get_percent(duree_count, lenght)
                  
            if not line[217:223].isdigit():
                lien_count += 1
                dic["Lien"] = anomalie_nombre("Le ", 'lien', line[217:223])                + get_percent(lien_count, lenght)
                          
            if not line[225:229].isdigit():
                passage_count += 1
                dic["Nombre de passage"] = anomalie_nombre("Le ", "nombre de passage", line[225:229])                + get_percent(passage_count, lenght)
                  
    if not lines[-1][30:54].isdigit():
        dic["Enregistrement 99"] = anomalie_nombre("L'", "enregistrement 99", lines[-1][30:54])
    
    return dic


# In[20]:


def read_file(file_path):
    
    try:
        with open(file_path, 'r', encoding = 'latin-1') as file :
            content = file.read()
    
        lines = content.split('\n')
        name = file_path[file_path.rfind('\\') + 1:]
        
        if  len(lines) > 7 and len(lines[0]) > 34 and lines[0][30:34] == "DIP4":
            is_dip4 = True
            code = lines[0][:3]
            chaine = lines[0][35:85].strip()
            
        else:
            is_dip4 = False
            
    except PermissionError:
        is_dip4 = False
        
    if is_dip4:
        lines = [line for line in lines if line[25:27] == "10"]
        dic = check_dip4(lines, code, chaine, name, file_path)
    else:
        dic = None

        
    return dic


# In[21]:


def browse(dirs):

    dir_list = list()
    dic_list = list()
    ext_list = [".xls", ".xlsx", ".xlsm", ".csv", ".xls", ".xlt", ".efi", ".pyd", ".tcl",                ".msg", ".mov", ".zip", ".ipynb", ".7z", ".py", ".dat", ".dll", ".exe", ".ini",                ".avi", ".mp3", ".mpeg", ".jpeg", ".gif", ".jpg", ".png", ".mpg", ".mpa",                ".wma", ".asf", ".rare", ".mp2", ".m2p", ".vob", ".dif", ".riff", ".wav",                ".bwf", ".ogg", ".aiff", ".caf", ".raw", ".flac", ".alac", ".aac", ".mxp4",                ".pct", ".bmp", '.tif', ".ai", ".swf", ".ppt", ".doc", ".docx", ".pptx", ".msi"]
    
    for directory in dirs:
        
        #unzip_all(directory)
        try:
            for path in tqdm(os.listdir(directory), desc = directory):

                file_path = os.path.join(directory, path) 

                if os.path.isdir(file_path):
                    dir_list.append(file_path)

                elif not path.lower()[path.rfind('.'):] in ext_list:
                    dic_list.append(read_file(file_path))
                    
        except PermissionError:
            continue
    
    if dir_list == []:
        dir_list = None
        
    return dir_list, dic_list    
    


# In[22]:


def search_dip4():
    path = input('Donnez-moi le chemin d\'acces à vos fichiers dip4 ?\n')
    if not path:
        sys.exit()
        
    dirs = [path]
    files = list()
    df_list = list()
    dic_list = list()

    
    while dirs is not None:
        dirs, dics = browse(dirs)
        dic_list += [dic for dic in dics]
    
    dip4_dic_list = [item for item in dic_list if item is not None]
    
    if not dip4_dic_list:
        print("Aucun fichier DIP4 trouve.")
        os.system("pause")
        return None
    
    df = pd.DataFrame(dip4_dic_list)
    
    cols = ["Nom du fichier", "Nom de la chaîne", "Code Declarant", "Longueur des lignes", "Numero d'ordre",            "Date de debut de diffusion", "Heure de diffusion", "Type de titre", "Titre",            "Numero de l'episode", "Genre de diffusion de l'oeuvre",  "Duree de diffusion",            "Lien", "Nombre de passage", "Enregistrement 99", "Extension", "Chemin complet"]
    
    df = df[cols].sort_values('Chemin complet')
    
    cols = ["Longueur des lignes", "Numero d'ordre", "Date de debut de diffusion", "Heure de diffusion", "Type de titre", "Titre", "Numero de l'episode", "Genre de diffusion de l'oeuvre", "Duree de diffusion", "Lien", "Nombre de passage"]
    
    for col in tqdm(df[cols]):
        df[col + "_pourcentage"] = df[col].apply(separate_percent)
        df[col] = df[col].apply(erase_percent)
        
    cols = ["Nom du fichier", "Nom de la chaîne", "Code Declarant", "Longueur des lignes", "Longueur des lignes_pourcentage", "Numero d'ordre", "Numero d'ordre_pourcentage", "Date de debut de diffusion", "Date de debut de diffusion_pourcentage", "Heure de diffusion", "Heure de diffusion_pourcentage", "Type de titre", "Type de titre_pourcentage", "Titre", "Titre_pourcentage", "Numero de l'episode", "Numero de l'episode_pourcentage", "Genre de diffusion de l'oeuvre", "Genre de diffusion de l'oeuvre_pourcentage", "Duree de diffusion", "Duree de diffusion_pourcentage", "Lien", "Lien_pourcentage", "Nombre de passage", "Nombre de passage_pourcentage", "Enregistrement 99", "Chemin complet"]
    
    folder = input('Choisissez un dossier pour sauvegarder la liste des fichiers DIP4 trouves\n')
    
    if not folder:
        df[cols].to_csv("liste_dip4.csv", index = False, sep = ';', encoding = 'latin-1')
        
    df[cols].to_csv(os.path.join(folder, "liste_dip4.csv"), index = False, sep = ';', encoding = 'latin-1')
    
    print("liste_dip4.csv créé dans " + folder + '.')
    
    print('"liste_dip4.csv" a ete créé dans "' + folder + '" avec succès.')

    return df[cols]


# # 1. Main

# In[23]:


def main():
    res = input('Voulez-vous rechercher des fichiers dip4 ?\n')
    if res in ['Y', 'y', 'yes', 'Yes', 'oui', 'Oui', 'OUI', 'o', 'O', 'YES']:
        search_dip4()
    else:
        pass
    res = input('Voulez-vous analyser les fichiers dip4 ?\n')
    if res in ['Y', 'y', 'yes', 'Yes', 'oui', 'Oui', 'OUI', 'o', 'O', 'YES']:
        parse_dip4(None)
    else:
        concatenate_final_csv('oeuvres', None) 
        concatenate_final_csv('ayants_droit', None)   


# In[ ]:


main()


# In[ ]:


#Users(/maximemartins/Desktop/testLight/liste_dip4.csv)

#Users(/maximemartins/Desktop/test, PUB/liste_dip4.csv)

#Users(/maximemartins/Desktop/test/liste_dip4.csv)


# In[ ]:




