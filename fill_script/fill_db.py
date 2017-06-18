#!/usr/bin/python
# -*- coding: utf-8 -*-
<<<<<<< HEAD
from db_manager import DBmanager
import pandas as pd
from random import randint


class DBfill:

    def __init__(self, cant_years, cant_arbitros, competidores_por_escuela, enfrentamientos_por_competidor):
        self.d = DBmanager()
        self.cant_years = cant_years
        self.cant_arbitros = cant_arbitros
        self.competidores_por_escuela = competidores_por_escuela
        self.enfrentamientos_por_competidor = enfrentamientos_por_competidor
        self.nextId = 0
        self.nextDNI = 30000000

    def load_db(self):
        # Este metodo resetea la base de datos
        # El orden en que se cargan las tablas es importante
        self.d.create_db(override=True)
        self.load_campeonato()
        self.load_arbitros()
        self.load_escuelas()
        self.load_competidores()
        self.load_categorias()
        self.load_enfrentamientos()

    def getNewId(self):
        self.nextId += 1
        return self.nextId

    def getNewDNI(self):
        self.nextDNI += 1
        return self.nextDNI

    def load_campeonato(self):
        print('Cargando campeonatos')
        for y in range(self.cant_years):
            self.d.insert_campeonato(2017 - y)

    def load_arbitros(self):
        print('Cargando arbitros')
        nombres = pd.read_csv('../csv/nombres.csv', sep=',')
        campeonatos = self.d.query('Campeonato').items
        for a in range(self.cant_arbitros):
            dni = self.getNewDNI()
            self.d.insert_arbitro(dni, nombres['Nombre'][randint(0, len(nombres) - 1)])
            for c in campeonatos:
                self.d.add_campeonato_to_arbitro(dni, c['yearCampeonato'])

    def load_escuelas(self):
        print('Cargando Escuelas...')
        escuelas = pd.read_csv('../csv/escuelas.csv', sep=',')
        for e in escuelas.iterrows():
            self.d.insert_escuela(e[1]['Nombre'])

    def load_competidores(self):
        print('Cargando Competidores...')
        nombres = pd.read_csv('../csv/nombres.csv', sep=',')
        escuelas = self.d.query('Escuela').items
        campeonatos = self.d.query('Campeonato').items
        for e in escuelas:
            for n in range(self.competidores_por_escuela):
                dni = self.getNewDNI()
                nombre = nombres['Nombre'][randint(0, len(nombres) - 1)]
                self.d.insert_competidor(dni, nombre, e['NombreEscuela'])
                self.d.add_competidor_to_escuela(e['NombreEscuela'], dni)
                for c in campeonatos:
                    self.d.add_competidor_to_campeonato(c['yearCampeonato'], dni, e['NombreEscuela'])

            for c in campeonatos:
                self.d.add_escuela_to_campeonato(c['yearCampeonato'], e['NombreEscuela'])
                self.d.add_campeonato_to_escuela(e['NombreEscuela'], c['yearCampeonato'])

    def load_categorias(self):
        categorias = pd.read_csv('../csv/categorias.csv', sep=',')
        competidores = self.d.query('Competidor').items
        campeonatos = self.d.query('Campeonato').items
        for campeonato in campeonatos:
            for c in categorias.iterrows():
                idCategoria = self.getNewId()
                print('Insertando categoria: ', c[1]['Nombre'])
                indexes = range(len(competidores))
                index = randint(0, len(indexes) - 1)
                oro = {
                    'DNICompetidor': competidores[index]['DNICompetidor'],
                    'NombreEscuela': competidores[index]['NombreEscuela']
                }
                self.d.add_medalla_to_competidor(competidores[index]['DNICompetidor'], 'Oro', idCategoria)
                self.d.add_medalla_to_competidor_in_escuela(competidores[index]['NombreEscuela'],
                                                            competidores[index]['DNICompetidor'], 'Oro')
                indexes.pop(index)
                index = randint(0, len(indexes) - 1)
                bronce = {
                    'DNICompetidor': competidores[index]['DNICompetidor'],
                    'NombreEscuela': competidores[index]['NombreEscuela']
                }
                self.d.add_medalla_to_competidor(competidores[index]['DNICompetidor'], 'Bronce', idCategoria)
                self.d.add_medalla_to_competidor_in_escuela(competidores[index]['NombreEscuela'],
                                                            competidores[index]['DNICompetidor'], 'Bronce')
                indexes.pop(index)
                index = randint(0, len(indexes) - 1)
                plata = {
                    'DNICompetidor': competidores[index]['DNICompetidor'],
                    'NombreEscuela': competidores[index]['NombreEscuela']
                }
                self.d.add_medalla_to_competidor(competidores[index]['DNICompetidor'], 'Plata', idCategoria)
                self.d.add_medalla_to_competidor_in_escuela(competidores[index]['NombreEscuela'],
                                                            competidores[index]['DNICompetidor'], 'Plata')
                self.d.add_categoria_to_campeonato(campeonato['yearCampeonato'], idCategoria, c[1]['Nombre'],
                                                   oro, plata, bronce)

    def load_enfrentamientos(self):
        campeonatos = self.d.query('Campeonato').items
        competidores = self.d.query('Competidor').items
        for campeonato in campeonatos:
            for i in range(len(competidores)):
                first = i + self.competidores_por_escuela
                last = i + self.competidores_por_escuela + self.enfrentamientos_por_competidor
                for j in range(first, min(last, len(competidores))):
                    if randint(0, 1) == 1:  # Gana o pierde de forma random uniforme
                        self.d.insert_enfrentamiento(self.getNewId(), campeonato['yearCampeonato'],
                                                     competidores[i]['DNICompetidor'], competidores[j]['DNICompetidor'])
                    else:
                        self.d.insert_enfrentamiento(self.getNewId(), campeonato['yearCampeonato'],
                                                     competidores[j]['DNICompetidor'], competidores[i]['DNICompetidor'])


if __name__ == '__main__':
    f = DBfill(3, 10, 10, 2)
    f.load_db()
=======

import rethinkdb as r
from rethinkdb.errors import *


class TKDDBgenerator:

    def __init__(self):
        self.connection = r.connect("localhost", 28015)
        self.db_name = 'Itf_TKD_WC_historico'
        self.table_infos = {
            'Arbitro': {'name': 'Arbitro', 'pk': 'DNIArbitro'},
            'Categoria': {'name': 'Categoria', 'pk': 'idCategoria'},
            'Escuela': {'name': 'Escuela', 'pk': 'NombreEscuela'},
            'Campeonato': {'name': 'Campeonato', 'pk': 'yearCampeonato'},
            'Enfrentamiento': {'name': 'Enfrentamiento', 'pk': 'idEnfrentamiento'},
            'Competidor': {'name': 'Competidor', 'pk': 'DNICompetidor'},
        }

    def create_db(self, override=False):
        try:
            print('Creando base de datos: ', self.db_name)
            r.db_create(self.db_name).run(self.connection)
        except ReqlOpFailedError:
            if override:
                print('La base de datos: ', self.db_name, ' ya existe, sobre-escibiendo')
                self.drop_db()
                print('Creando base de datos: ', self.db_name)
                r.db_create(self.db_name).run(self.connection)
            else:
                print('La base de datos ya existe, si desea forzar la sobreescritura, use el parametro override=True')
                return None

        for table in self.table_infos.itervalues():
            print('Creando tabla: ', table['name'])
            r.db(self.db_name).table_create(table['name'], primary_key=table['pk']).run(self.connection)
        print('Base de datos: ', self.db_name, ' Creada')

    def empty_table(self, table_name):

        if table_name in self.table_infos.keys():
            table = self.table_infos[table_name]
            try:
                r.db(self.db_name).table_drop(table['name']).run(self.connection)
                r.db(self.db_name).table_create(table['name'], primary_key=table['pk']).run(self.connection)
            except ReqlOpFailedError:
                print('La tabla ' + tabla['name'] + ' no existe')
        else:
            print('La tabla especificada no forma parte de la base de datos')

    def drop_db(self):
        try:
            print('Eliminando la base de datos: ', self.db_name)
            r.db_drop(self.db_name).run(self.connection)
            print('Base de datos: ', self.db_name, ' Eliminada')
        except ReqlOpFailedError:
            print('La base de datos: ', self.db_name, ' No existe')

    def insert(self, table_name, register):
        r.db(self.db_name) \
            .table(table_name) \
            .insert(register) \
            .run(self.connection)
        print('Insertado correctamente: ', register, table_name)

    def append(self, table_name, pk_value, attribute, value):
        if table_name in self.table_infos.keys():
            table = self.table_infos[table_name]
            r.db(self.db_name) \
                .table(table['name']) \
                .filter(r.row[table['pk']] == pk_value) \
                .update({attribute: r.row[attribute].append(value)}) \
                .run(self.connection)
        else:
            print('La tabla especificada no forma parte de la base de datos')

    def update(self, table_name, pk_value, attribute, value):
        if table_name in self.table_infos.keys():
            table = self.table_infos[table_name]
            r.db(self.db_name) \
                .table(table['name']) \
                .filter(r.row[table['pk']] == pk_value) \
                .update({attribute: value}) \
                .run(self.connection)
        else:
            print('La tabla especificada no forma parte de la base de datos')

    # Specific table insert update append methods

    def insert_arbitro(self, DNIArbitro, NombreArbitro, ListaCampeonatos=[]):
        new_document = {
            'DNIArbitro': DNIArbitro,
            'NombreArbitro': NombreArbitro,
            'ListaCampeonatos': ListaCampeonatos
        }
        self.insert(self.table_infos['Arbitro']['name'], new_document)

    def add_campeonato_to_arbitro(self, DNIArbitro, yearCampeonato):
        self.append('Arbitro', DNIArbitro, 'ListaCampeonatos', yearCampeonato)

    def insert_categoria(self, idCategoria, Modalidad,
                         GanadorOro={}, GanadorPlata={}, GanadorBronce={}):
        new_document = {
            'idCategoria': idCategoria,
            'Modalidad': Modalidad,
            'GanadorOro': GanadorOro,
            'GanadorPlata': GanadorPlata,
            'GanadorBronce': GanadorBronce
        }
        self.insert(self.table_infos['Categoria']['name'], new_document)

    def update_medalla_from_categoria(self, idCategoria, medalla, DNICompetidor, NombreEscuela):
        new_document = {
            'DNICompetidor': DNICompetidor,
            'NombreEscuela': NombreEscuela
        }
        self.update('Categoria', idCategoria, medalla, new_document)

    def insert_escuela(self, NombreEscuela, ListaCompetidores=[], ListaCampeonatos=[]):
        new_document = {
            'NombreEscuela': NombreEscuela,
            'ListaCompetidores': ListaCompetidores,
            'ListaCampeonatos': ListaCampeonatos
        }
        self.insert(self.table_infos['Escuela']['name'], new_document)

    def add_competidor_to_escuela(self, NombreEscuela, DNICompetidor, Oro, Plata, Bronce):
        new_document = {
            'DNICompetidor': DNICompetidor,
            'Oro': Oro,
            'Plata': Bronce,
            'Bronce': Plata
        }
        self.append('Escuela', NombreEscuela, 'ListaCompetidores', new_document)

    def add_campeonato_to_escuela(self, NombreEscuela, yearCampeonato):
        self.append('Escuela', NombreEscuela, 'ListaCampeonatos', yearCampeonato)

    def insert_campeonato(self, yearCampeonato, ListaCompetidores=[], ListaCategorias=[],
                          ListaEscuelas=[], ListaArbitros=[], ListaEnfrentamientos=[]):
        new_document = {
            'yearCampeonato': yearCampeonato,
            'ListaCompetidores': ListaCompetidores,
            'ListaCategorias': ListaCategorias,
            'ListaEscuelas': ListaEscuelas,
            'ListaArbitros': ListaArbitros,
            'ListaEnfrentamientos': ListaEnfrentamientos
        }
        self.insert(self.table_infos['Campeonato']['name'], new_document)

    def add_competidor_to_campeonato(self, yearCampeonato, DNICompetidor, NombreEscuela):
        new_document = {
            'DNICompetidor': DNICompetidor,
            'NombreEscuela': NombreEscuela
        }
        self.append('Campeonato', yearCampeonato, 'ListaCompetidores', new_document)

    def add_categoria_to_campeonato(self, yearCampeonato, idCategoria,
                                    Modalidad, GanadorOro, GanadorPlata, GanadorBronce):
        new_document = {
            'idCategoria': idCategoria,
            'Modalidad': Modalidad,
            'GanadorOro': GanadorOro,
            'GanadorPlata': GanadorPlata,
            'GanadorBronce': GanadorBronce
        }
        self.append('Campeonato', yearCampeonato, 'ListaCategorias', new_document)

    def add_escuela_to_campeonato(self, yearCampeonato, NombreEscuela):
        self.append('Campeonato', yearCampeonato, 'ListaEscuelas', NombreEscuela)

    def add_arbitro_to_campeonato(self, yearCampeonato, DNIArbitro):
        self.append('Campeonato', yearCampeonato, 'ListaArbitros', DNIArbitro)

    def add_enfrentamiento_to_campeonato(self, yearCampeonato, idEnfrentamiento):
        self.append('Campeonato', yearCampeonato, 'ListaEnfrentamientos', idEnfrentamiento)

    def insert_enfrentamiento(self, idEnfrentamiento, yearCampeonato):
        new_document = {
            'idEnfrentamiento': idEnfrentamiento,
            'yearCampeonato': yearCampeonato
        }
        self.insert(self.table_infos['Enfrentamiento']['name'], new_document)

    def insert_competidor(self, DNICompetidor, NombreCompetidor, NombreEscuela,
                          EnfrentamientosPerdidos=[], EnfrentamientosGanados=[], Oro=[], Plata=[], Bronce=[]):
        new_document = {
            'DNICompetidor': DNICompetidor,
            'NombreCompetidor': NombreCompetidor,
            'NombreEscuela': NombreEscuela,
            'EnfrentamientosPerdidos': EnfrentamientosPerdidos,
            'EnfrentamientosGanados': EnfrentamientosGanados,
            'Oro': Oro,
            'Plata': Plata,
            'Bronce': Bronce
        }
        self.insert(self.table_infos['Competidor']['name'], new_document)

    def add_medalla_to_competidor(self, DNICompetidor, medalla, idCategoria):
        self.append('Competidor', DNICompetidor, medalla, idCategoria)

    def add_enfrentamiento_ganado_to_competidor(self, DNICompetidor, idEnfrentamiento):
        self.append('Competidor', DNICompetidor, 'EnfrentamientosGanados', idEnfrentamiento)

    def add_enfrentamiento_perdido_to_competidor(self, DNICompetidor, idEnfrentamiento):
        self.append('Competidor', DNICompetidor, 'EnfrentamientosPerdidos', idEnfrentamiento)
>>>>>>> 2fc21311a226df8f9ca7605088d72b70a3884488
