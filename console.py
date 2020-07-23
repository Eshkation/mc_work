class Console:
    def __init__(self, prefix):
        self.set_prefix(prefix)
        self.tags = {
            'B': 'BLUE',
            'C': 'CYAN',
            'G': 'GREEN',
            'M': 'MAGENTA',
            'R': 'RED',
            'W': 'WHITE',
            'Y': 'YELLOW',
            'LB': 'LIGHTBLUE_EX',
            'LC': 'LIGHTCYAN_EX',
            'LG': 'LIGHTGREEN_EX',
            'LM': 'LIGHTMAGENTA_EX',
            'LR': 'LIGHTRED_EX',
            'LW': 'LIGHTWHITE_EX',
            'LY': 'LIGHTYELLOW_EX',
        }
        self.sql_keywords = [
            'ABORT', 'ACTION', 'ADD', 'AFTER', 'ALL', 'ALTER',
            'ANALYZE', 'AND', 'AS', 'ASC', 'ATTACH', 'AUTOINCREMENT', 'BEFORE',
            'BEGIN', 'BETWEEN', 'BY', 'CASCADE', 'CASE', 'CAST', 'CHECK',
            'COLLATE', 'COLUMN', 'COMMIT', 'CONFLICT', 'CONSTRAINT', 'CREATE',
            'CROSS', 'CURRENT', 'CURRENT_DATE', 'CURRENT_TIME',
            'CURRENT_TIMESTAMP', 'DATABASE', 'DEFAULT', 'DEFERRABLE',
            'DEFERRED', 'DELETE', 'DESC', 'DETACH', 'DISTINCT', 'DO', 'DROP',
            'EACH', 'ELSE', 'END', 'ESCAPE', 'EXCEPT', 'EXCLUSIVE', 'EXISTS',
            'EXPLAIN', 'FAIL', 'FILTER', 'FOLLOWING', 'FOR', 'FOREIGN', 'FROM',
            'FULL', 'GLOB', 'GROUP', 'HAVING', 'IF', 'IGNORE', 'IMMEDIATE',
            'IN', 'INDEX', 'INDEXED', 'INITIALLY', 'INNER', 'INSERT',
            'INSTEAD', 'INTERSECT', 'INTO', 'IS', 'ISNULL', 'JOIN', 'KEY',
            'LEFT', 'LIKE', 'LIMIT', 'MATCH', 'NATURAL', 'NO', 'NOT',
            'NOTHING', 'NOTNULL', 'NULL', 'OF', 'OFFSET', 'ON', 'OR', 'ORDER',
            'OUTER', 'OVER', 'PARTITION', 'PLAN', 'PRAGMA', 'PRECEDING',
            'PRIMARY', 'QUERY', 'RAISE', 'RANGE', 'RECURSIVE', 'REFERENCES',
            'REGEXP', 'REINDEX', 'RELEASE', 'RENAME', 'REPLACE', 'RESTRICT',
            'RIGHT', 'ROLLBACK', 'ROW', 'ROWS', 'SAVEPOINT', 'SELECT', 'SET',
            'TABLE', 'TEMP', 'TEMPORARY', 'THEN', 'TO', 'TRANSACTION',
            'TRIGGER', 'UNBOUNDED', 'UNION', 'UNIQUE', 'UPDATE',
            'USING', 'VACUUM', 'VALUES', 'VIEW', 'VIRTUAL',
            'WHEN', 'WHERE', 'WINDOW', 'WITH', 'WITHOUT'
        ]
        self.sql_keywords = re.compile(
            '\\b' + '\\b|\\b'.join(map(re.escape, self.sql_keywords)) + '\\b')
        self.sql_types = [
            'CHARACTER', 'CHAR', 'VARCHAR', 'BOOLEAN', 'SMALLINT', 'INTEGER',
            'INT', 'DECIMAL', 'NUMERIC', 'REAL', 'FLOAT', 'DOUBLE PRECISION',
            'DATE', 'TEXT', 'TIME', 'TIMESTAMP', 'CLOB', 'BLOB', 'DATETIME'
        ]
        re_types = []
        for data_type in self.sql_types:
            re_types.append(r'\b({0})(\((.*)\)|)\b'.format(data_type))
        self.sql_types = '|'.join(re_types)

    def set_prefix(self, prefix):
        self.prefix = '~\\{0}'.format(prefix.replace('.', '\\'))

    def generate_prefix(self, caller, p_type):
        current_date = datetime.datetime.now().strftime('%H:%M:%S')
        output_prefix = u'<G>→ <LC>{0} <C>[{1} {2}<C>]<M>› <W>{{0}}'.format(
            self.prefix + '\\' + caller,
            current_date,
            p_type,
        )
        return self.parse_color_tag(output_prefix)

    def parse_color_tag(self, message):
        message = str(message)
        output = re.sub(r'<([A-Z]+)>',
                        lambda color_tag: self.get_color_from_tag(
                            color_tag.group(1)),
                        message)
        return output

    def get_color_from_tag(self, tag):
        if (tag in self.tags):
            return getattr(colorama.Fore, self.tags[tag])

        elif (tag.replace('#', '') in self.tags):
            return getattr(colorama.Back, self.tags[tag])

        return '<{0}>'.format(tag)

    def info(self, message, minified=False, tabs=0, overwrite=False):
        self.generate_print(
            '<LC>INFO ~',
            message, inspect.stack()[1].function, minified, tabs, overwrite)

    def warning(self, message, minified=False, tabs=0, overwrite=False):
        self.generate_print(
            '<LY>WARNING #',
            message, inspect.stack()[1].function, minified, tabs, overwrite)

    def error(self, message, minified=False, tabs=0, overwrite=False):
        self.generate_print(
            '<R>ERROR !',
            message, inspect.stack()[1].function, minified, tabs, overwrite)

    def success(self, message, minified=False, tabs=0, overwrite=False):
        self.generate_print(
            '<G>SUCCESS •',
            message, inspect.stack()[1].function, minified, tabs, overwrite)

    def print_json(self, message, minified=True, tabs=0, prefix=None):
        if (type(message) != str):
            message = json.dumps(message, indent=2).replace('  ', '\t')

        message = re.sub('"(.*)":',
                         lambda keyword: '"<LC>{0}<W>":'.format(
                             keyword.group(1)),
                         message)
        message = re.sub(': "(.*)"',
                         lambda keyword: ': <LG>"{0}"<W>'.format(
                             keyword.group(1)),
                         message)
        message = re.sub(r': (\d+|true|false)',
                         lambda keyword: ': <LB>{0}<W>'.format(
                             keyword.group(1)),
                         message)
        message = re.sub(r'\t"(.*?)"([,$\s])',
                         lambda keyword: '\t<LG>"{0}"<W>{1}'.format(
                             keyword.group(1),
                             keyword.group(2)),
                         message)
        message = re.sub(r'\t(\d+|true|false)([,$\s])',
                         lambda keyword: '\t<LB>{0}<W>{1}'.format(
                             keyword.group(1),
                             keyword.group(2)),
                         message)
        self.generate_print(
            prefix or '<LC>JSON -',
            message,
            inspect.stack()[1].function,
            minified,
            tabs
        )

    def print_sql(self, message, minified=True, tabs=0, prefix=None):
        message = self.sql_keywords.sub(
            lambda keyword: '<Y>{0}<W>'.format(
                keyword.group(0)
            ),
            message
        )
        message = re.sub(self.sql_types,
                         lambda keyword: '<LB>{0}<W>'.format(
                             keyword.group(0).replace(
                                 ')', '<LB>)<W>').replace('(', '<LB>(<W>')),
                         message)
        message = re.sub(r'(`|"|\')(.*?)(`|"|\')',
                         lambda string: '<LG>{0}<W>'.format(
                             string.group(0)),
                         message)
        message = re.sub(r'(\b\d+)',
                         lambda string: '<LM>{0}<W>'.format(
                             string.group(0)),
                         message)
        self.generate_print(
            prefix or '<LC>SQL -', message, inspect.stack()[
                1].function, minified, tabs
        )

    def generate_print(self, prefix, message, caller, minified, tabs=0,
                       overwrite=False):
        if (minified):
            output = self.parse_color_tag('{0}<M>{1}<W>{{0}}'.format(
                tabs * '\t',
                tabs == 0 and '› ' or ''
            ))
        else:
            output = self.generate_prefix(caller, prefix.replace(' ', ' <LB>'))

        output = (output.format(
            self.parse_color_tag(message.replace('\n', '\n' + (tabs * '\t')))
        ) + colorama.Style.RESET_ALL
        ).replace('\t', '    ')

        if (overwrite):
            print(output, end='\r')
        else:
            sys.stdout.write('\033[K')
            print(output)
