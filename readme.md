graph TD
    %% Define Styles
    classDef source fill:#2563eb,stroke:#1d4ed8,color:#fff,font-weight:bold;
    classDef adf fill:#f59e0b,stroke:#d97706,color:#fff,font-weight:bold;
    classDef sink fill:#10b981,stroke:#059669,color:#fff,font-weight:bold;

    %% Source Database Section
    subgraph Azure_SQL_Database ["Azure SQL Database (Source & Metadata)"]
        V[Presentation View: v_Customer_Transactions]:::source
        W[Control Table: WatermarkTable]:::source
        SP[Stored Procedure: usp_write_watermark]:::source
    end

    %% ADF Orchestration Section
    subgraph ADF_Pipeline ["Azure Data Factory Orchestration Engine"]
        L1[Lookup 1: Lkp_get_old_watermark]:::adf
        L2[Lookup 2: Lkp_get_new_watermark]:::adf
        C[Copy Data Activity: copy_delta_data]:::adf
    end

    %% Target Data Lake Section
    subgraph Azure_Data_Lake ["Target Data Lake (ADLS Gen2)"]
        P[landing/raw/Customer_transactions/]:::sink
    end

    %% Flow Connections
    W -->|1. Fetches Old Timestamp| L1
    V -->|2. Calculates New Max Timestamp| L2
    
    L1 -->|3a. Passes Baseline Bound| C
    L2 -->|3b. Passes Ceiling Bound| C
    V -->|3c. Streams Delta Dataset 1,000,000+ Rows| C
    
    C -->|4. Writes Optimized Columnar Payload| P
    C -->|5. On Success Trigger| SP
    SP -->|6. Commits New Checkpoint to Database| W

    %% Layout Tweaks
    style Azure_SQL_Database fill:#f0fdf4,stroke:#bbf7d0,stroke-width:2px;
    style ADF_Pipeline fill:#fff7ed,stroke:#ffedd5,stroke-width:2px;
    style Azure_Data_Lake fill:#eff6ff,stroke:#dbeafe,stroke-width:2px;