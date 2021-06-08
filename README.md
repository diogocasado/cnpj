# About this

This simple command line script parses data from Brazil's CNPJ Public Data and exports to an usable format.
 
Source:
[Download](https://receita.economia.gov.br/orientacao/tributaria/cadastros/cadastro-nacional-de-pessoas-juridicas-cnpj/dados-publicos-cnpj)

# How it works

The utility will process batch files by reading the records sequentially and updating an internal data structure.
As soon as new `record 1` is identified, the previous (now complete) data structure will be evaluated and an output will be generated.

**Sample internal data strutcture:**
```json
{
   "cnpj":"0000000000000",
   "razaoSocial":"***** ** ****** **",
   "nomeFantasia":"MANAUS (AM)",
   "situacao":"02",
   "tipoEndereco":"RUA",
   "endereco":"GUILHERME MOREIRA",
   "number":"315",
   "bairro":"CENTRO",
   "cep":"69005300",
   "uf":"AM",
   "municipio":"MANAUS",
   "socios":[
      {
         "tipo":"2",
         "nome":"**** ******* *****",
         "nomeRepr":"CPF INVALIDO"
      },
      {
         "tipo":"2",
         "nome":"**** ******* *****",
         "nomeRepr":"CPF INVALIDO"
      }
   ]
}
```

Empty fields are not set.
Please refer to `DEFS` object in `index.js` file for a complete property list.


**Note:**
> Only CSV output is currently supported.
> You can match the records for a first pass filter using `-m` option.

# Usage

```
./cnpj ops (args) -opt (opt_args)

Operations
        help: Show this help
        parse: Parse batch import file

Options
        v: (flag) Verbose
        in: (string) Input file
        m: (string) Match records: 'field:startsWith; field:start-end'
        or: (flag) Match rules using OR
        t: (string) Output type: csv (default), json
        sep: (string) CSV field separator
        del: (string) CSV field delimiter
        csv: (string) CSV field list: fields, field
        ml: (flag) Output matching lines only
        out: (string) Output file
```

**Tip:** Use `npm link` to make it a global utility.

Example: `./cnpj parse -in K3241.K03200DV.D00422.L00001 -out myfile.csv -ml -m situacao:02,socios[].tipo:3`
