#!/usr/bin/python
# -*- coding: utf-8 -*-

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
        r.db(self.db_name).table(table_name).insert(register).run(self.connection)
        print('Insertado correctamente: ', register, table_name)

    def insert_arbitro(self, DNIArbitro, NombreArbitro):
        new_document = {
            'DNIArbitro': DNIArbitro,
            'NombreArbitro': NombreArbitro
        }
        self.insert(self.table_infos['Arbitro']['name'], new_document)

    def insert_categoria(self, idCategoria, Modalidad, GanadorOro, GanadorPlata, GanadorBronce):
        new_document = {
            'idCategoria': idCategoria,
            'Modalidad': Modalidad,
            'GanadorOro': GanadorOro,
            'GanadorPlata': GanadorPlata,
            'GanadorBronce': GanadorBronce
        }
        self.insert(self.table_infos['Categoria']['name'], new_document)

    def insert_escuela(self, NombreEscuela, ListaCompetidores, ListaCampeonatos):
        new_document = {
            'NombreEscuela': NombreEscuela,
            'ListaCompetidores': ListaCompetidores,
            'ListaCampeonatos': ListaCampeonatoso
        }
        self.insert(self.table_infos['Escuela']['name'], new_document)

    def insert_campeonato(self, yearCampeonato, ListaCompetidores, ListaCategorias):
        new_document = {
            'yearCampeonato': NombreEscuela,
            'ListaCompetidores': ListaCompetidores,
            'ListaCategorias': ListaCategorias
        }
        self.insert(self.table_infos['Campeonato']['name'], new_document)

    def insert_enfrentamiento(self, idEnfrentamiento, yearCampeonato):
        new_document = {
            'idEnfrentamiento': idEnfrentamiento,
            'yearCampeonato': yearCampeonato
        }
        self.insert(self.table_infos['Enfrentamiento']['name'], new_document)

    def insert_competidor(self,):
        new_document = {
            'DNICompetidor': DNICompetidor,
            'NombreCompetidor': NombreCompetidor,
            'NombreEscuela': NombreEscuela,
            'Oro': Oro,
            'Plata': Plata,
            'Bronce': Bronce
        }
        self.insert(self.table_infos['Competidor']['name'], new_document)
