#!/usr/bin/env python
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.sql import text

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
        stats= NeverSourceSize(self.sourceId, nodeId, size, time)
        session.merge(stats)

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
    def prepare_records(self, records):

        for record in records:
            print ""

    def query(self):
        engine = create_engine(self.build_connection_string())
        data = dict()
        col_dict, column_list = NeverSource.build_column_list(self.columns)
        query_string = text("Select %s from %s" % (column_list[self.name], self.tableName))
        main_results = engine.execute(query_string).fetchall()
        i = 0
        for col in self.columns:
            if col.isPrimaryKey:
                primaryKey = col.name

        for result in main_results:
            i+=1
            row = dict()
            for col in col_dict[self.name]:
                row[col] = result[col]
            data[result[primaryKey]] = row
            if (i == 1):
                print result[primaryKey]
        #print str(i)
        #print len(data)
        for join_source in self.joinSources:
            # FIX ME ABSENT COLUMNS
            #print (join_source)
            join_query = text("Select %s from %s" % (column_list[join_source.name], join_source.tableName))
            #if join_source.source is not self:
            con_str = join_source.build_connection_string()
            #print con_str
            local_engine = create_engine(con_str)
            results = local_engine.execute(join_query).fetchall()
            local_engine.dispose()
            #else:
            #print str(engine)
            #    results = engine.execute(join_query).fetchall()

            for result in results:
                pk = result[join_source.primaryKey]
                if pk in data:
                    for col in col_dict[join_source.name]:
                        data[pk][col] = result[col]
        engine.dispose()
        return data

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
            if column.joinSource is not None:
                if column.joinSource.name not in col_dict:
                    col_dict[column.joinSource.name] = list()
                col_dict[column.joinSource.name].append(str(column.name))
                col_dict["SOURCES"][column.joinSource.name] = column.joinSource.name
            else:
                col_dict[column.source.name].append(str(column.name))
                col_dict["SOURCES"][column.source.name] = column.source.name

        col_lists = dict()
        for key in col_dict["SOURCES"]:
            col_lists[key] = ",".join(col_dict[key])

        return col_dict, col_lists


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

    def __init__(self, sourceId, nodeId, size, timing):
        self.sourceId = sourceId
        self.nodeId = nodeId
        self.size = size
        self.timing = timing

