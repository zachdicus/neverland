#!/usr/bin/env python
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pandas
import datetime

# Create the class base
Base = declarative_base()


class NeverSource(Base):

    __tablename__ = "Source"
    sourceId = Column("SourceId", Integer, primary_key=True)
    name = Column("Name", String)
    sourceTypeId = Column("SourceTypeId", Integer, ForeignKey("SourceType.SourceTypeId"))
    description = Column("Description", String)
    isActive = Column("IsActive", Integer)
    priority = Column("Priority", Integer)
    dbId = Column("DbId", Integer, ForeignKey("Database.DbId"))
    primaryNodeId = Column("PrimaryNodeId", Integer, ForeignKey("Node.NodeId"))
    secondaryNodeId = Column("SecondaryNodeId", Integer, ForeignKey("Node.NodeId"))
    schema = Column("Schema", String)
    tableName = Column("TableName", String)
    database = relationship("NeverDatabase", foreign_keys="[NeverSource.dbId]")
    sourceType = relationship("NeverSourceType", foreign_keys="[NeverSource.sourceTypeId]")
    primaryNode = relationship("NeverNode", foreign_keys="[NeverSource.primaryNodeId]")
    secondaryNode = relationship("NeverNode", foreign_keys="[NeverSource.secondaryNodeId]")
    columns = relationship("NeverColumn")
    joinSources = relationship("NeverJoinSource")

    def __init__(self, sourceId):
        self.sourceId = sourceId

    def insert_stats(self, session, nodeId, size, time):
        stats = NeverSourceSize(self.sourceId, nodeId, size, time, datetime.datetime.now())
        #session.merge(stats)
        session.add(stats)

    def __repr__(self):
        return "<Source(SourceId='%d', Name='%s', Description='%s', IsActive='%d', Priority='%d', Schema='%s'," \
               "Database(Host='%s', Port='%s', Username='%s', Password='%s'), " \
               "SourceTpe(Name='%s', AlchemyType='%s', Description='%s'))>" % (self.sourceId, self.name,
                                                                               self.description, self.isActive,
                                                                               self.priority, self.schema,
                                                                               self.database.host, self.database.port,
                                                                               self.database.username, self.database.password,
                                                                               self.sourceType.name, self.sourceType.alchemyType,
                                                                               self.sourceType.description)

    @staticmethod
    def data_frame(query, columns):
        """
        Takes a sqlalchemy query and a list of columns, returns a dataframe.
        """

        def make_row(x):
            return dict([(c, getattr(x, c)) for c in columns])

        return pandas.DataFrame([make_row(x) for x in query])

    def query(self):
        engine = create_engine(self.build_connection_string())
        data = dict()
        col_dict, column_list = NeverSource.build_column_list(self.columns)
        print column_list
        query_string = text("Select %s from %s" % (column_list[self.name], self.tableName))
        main_results = engine.execute(query_string)
        df = pandas.DataFrame(main_results.fetchall())
        df.columns = main_results.keys()

        for column in self.columns:
            if column.isPrimaryKey:
                primaryKey = column.name

        for join_source in self.joinSources:
            # FIXME ABSENT COLUMNS
            join_query = text("Select %s from %s" % (column_list[join_source.name], join_source.tableName))
            print "Sql %s" % join_query
            con_str = join_source.build_connection_string()

            local_engine = create_engine(con_str)
            results = local_engine.execute(join_query)
            localframe = pandas.DataFrame(results.fetchall())
            localframe.columns = results.keys()
            print "Key = %s, Join Key = %s" % (join_source.primaryKey, join_source.joinKey.name)

            # Determine the left and right key

            df = df.merge(localframe, how="outer", left_on=join_source.joinKey.name, right_on=join_source.primaryKey)
            local_engine.dispose()

        engine.dispose()
        filters = self.create_filters(df)
        return df, filters

    def build_connection_string(self):
        return ("%s://%s:%s@%s:%s/%s" % (self.sourceType.alchemyType, self.database.username,
                                         self.database.password, self.database.host,
                                         self.database.port, self.schema))

    @staticmethod
    def create_connection_string(engine_type, username, password, host, port, schema):
        return "%s://%s:%s@%s:%s/%s" % (engine_type, username, password, host, port, schema)

    @staticmethod
    def build_column_list(columns):
        col_dict = dict()
        col_dict["SOURCES"] = dict()
        for column in columns:
            if column.source.name not in col_dict:
                col_dict[column.source.name] = list()
            col_info = str(column.name)
            if column.rename:
                col_info = "%s as %s" % (column.name, column.rename)

            if column.joinSource is not None:
                if column.joinSource.name not in col_dict:
                    col_dict[column.joinSource.name] = list()
                col_dict[column.joinSource.name].append(col_info)#str(column.name))
                col_dict["SOURCES"][column.joinSource.name] = column.joinSource.name
            else:
                col_dict[column.source.name].append(col_info)#str(column.name))
                col_dict["SOURCES"][column.source.name] = column.source.name

        col_lists = dict()
        for key in col_dict["SOURCES"]:
            col_lists[key] = ",".join(col_dict[key])

        return col_dict, col_lists

    def create_filters(self, data):
        filters = dict()
        for column in self.columns:
            if column.isFilterable:
                filters[column.name] = pandas.unique(data[column.name].values)
            print "%s has %d" % (column.name, len(filters[column.name]))
        return filters


class NeverDatabase(Base):
    __tablename__ = "Database"

    dbId = Column("DbId", Integer, primary_key=True)
    host = Column("Host", String)
    port = Column("Port", Integer)
    username = Column("Username", String)
    password = Column("Password", String)

    def __init__(self, dbid):
        self.dbId = dbid


class NeverSourceType(Base):
    __tablename__ = "SourceType"

    sourceTypeId = Column("SourceTypeId", Integer, primary_key=True)
    name = Column("Name", String)
    alchemyType = Column("AlchemyType", String)
    description = Column("Description", String)

    def __init__(self, source_type_id):
        self.sourceTypeId = source_type_id


class NeverColumn(Base):
    __tablename__ = "Column"

    columnId = Column("ColumnId", Integer, primary_key=True)
    name = Column("Name", String)
    rename = Column("Rename", String)
    description = Column("Description", String)
    isFilterable = Column("IsFilterable", Integer)
    showFilters = Column("ShowAllFilters", Integer)
    showColumn = Column("ShowColumn", Integer)
    isPrimaryKey = Column("IsPrimaryKey", Integer)
    sourceId = Column("SourceId", Integer, ForeignKey("Source.SourceId"))
    displayName = Column("DisplayName", String)
    columnTypeId = Column("ColumnTypeId", Integer, ForeignKey("ColumnType.ColumnTypeId"))
    columnType = relationship("NeverColumnType", foreign_keys="[NeverColumn.columnTypeId]")
    source = relationship("NeverSource", foreign_keys="[NeverColumn.sourceId]")
    joinSourceId = Column("JoinSourceId", Integer, ForeignKey("JoinSource.JoinSourceId"))
    joinSource = relationship("NeverJoinSource", foreign_keys="[NeverColumn.joinSourceId]")

    def __init__(self, source_type_id):
        self.sourceTypeId = source_type_id


class NeverColumnType(Base):
    __tablename__ = "ColumnType"

    columnTypeId = Column("ColumnTypeId", Integer, primary_key=True)
    name = Column("Name", String)
    description = Column("Description", String)

    def __init__(self, source_type_id):
        self.sourceTypeId = source_type_id

class NeverNode(Base):
    __tablename__ = "Node"

    nodeId = Column("NodeId", Integer, primary_key=True)
    name = Column("Name", String)
    description = Column("Description", String)
    host = Column("Host", String)

    def __init__(self, source_type_id):
        self.sourceTypeId = source_type_id


class NeverJoinSource(Base):
    __tablename__ = "JoinSource"

    joinSourceId = Column("JoinSourceId", Integer, primary_key=True)
    name = Column("Name", String)
    description = Column("Description", String)
    tableName = Column("TableName", String)
    sourceId = Column("SourceId", Integer, ForeignKey("Source.SourceId"))
    source = relationship("NeverSource", foreign_keys="[NeverJoinSource.sourceId]")
    primaryKey = Column("PrimaryKey", String)
    joinKeyId = Column("JoinKeyId", Integer, ForeignKey("Column.ColumnId"))
    joinKey = relationship("NeverColumn", foreign_keys="[NeverJoinSource.joinKeyId]")
    dbId = Column("DbId", Integer, ForeignKey("Database.DbId"))
    database = relationship("NeverDatabase", foreign_keys="[NeverJoinSource.dbId]")
    sourceTypeId = Column("SourceTypeId", Integer, ForeignKey("SourceType.SourceTypeId"))
    sourceType = relationship("NeverSourceType", foreign_keys="[NeverJoinSource.sourceTypeId]")
    schema = Column("Schema", String)

    def __init__(self, source_type_id):
        self.sourceTypeId = source_type_id

    def __repr__(self):
        return "<JoinSource(SourceId='%d', Name='%s', Description='%s', IsActive='%d', Priority='%d', Schema='%s'," \
               "Database(Host='%s', Port='%s', Username='%s', Password='%s'), " \
               "SourceTpe(Name='%s', AlchemyType='%s', Description='%s'))>" % (self.sourceId, self.name,
                                                                               self.description, 1,
                                                                               1, 1,
                                                                               self.database.host, self.database.port,
                                                                               self.database.username, self.database.password,
                                                                               self.sourceType.name, self.sourceType.alchemyType,
                                                                               self.sourceType.description)
    def build_connection_string(self):
        return ("%s://%s:%s@%s:%s/%s" % (self.sourceType.alchemyType, self.database.username,
                                         self.database.password, self.database.host,
                                         self.database.port, self.schema))


class NeverSourceSize(Base):
    __tablename__ = "SourceSize"

    nodeId = Column("NodeId", Integer, primary_key=True)
    sourceId = Column("SourceId", Integer, primary_key=True)
    size = Column("Size", Integer)
    timing = Column("Timing", Integer)
    date = Column("SizeDate", DateTime)

    def __init__(self, sourceId, nodeId, size, timing, date):
        self.sourceId = sourceId
        self.nodeId = nodeId
        self.size = size
        self.timing = timing
        self.date = date
