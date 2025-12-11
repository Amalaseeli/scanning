CREATE TABLE dbo.Do_Co_Scanning_Data(
    DeviceID Nvarchar(50) NOT NULL,
    EntryNo Int NOT NULL,
    Barcode Nvarchar(50) NOT NULL,
    ScanDate DATE NOT NULL,
    ScanTime TIME NOT NULL,
    UserID Nvarchar(50) NULL,
    CONSTRAINT PK_Do_Co_Scanning_Data PRIMARY KEY CLUSTERED (DeviceID, EntryNo)

)