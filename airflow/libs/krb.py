from krbticket import KrbCommand, KrbConfig

class Kerberos:
    def __init__(self, principal, keytab, **kwargs):
        self.principal = principal
        self.keytab = keytab

    def kinit(self):
        kconfig = KrbConfig(principal=self.principal, keytab=self.keytab)
        KrbCommand.kinit(kconfig)

    def destroy(self):
        kconfig = KrbConfig(principal=self.principal, keytab=self.keytab)
        KrbCommand.kdestroy(kconfig)

    def __enter__(self):
        self.kinit()
        return self  # А точно надо возвращать?

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.destroy()
