namespace GStoreClient {
    partial class Form1 {
        /// <summary>
        ///  Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        ///  Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing) {
            if (disposing && (components != null)) {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        ///  Required method for Designer support - do not modify
        ///  the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent() {
            this.btReg = new System.Windows.Forms.Button();
            this.tbNick = new System.Windows.Forms.TextBox();
            this.tbPort = new System.Windows.Forms.TextBox();
            this.tbRegResult = new System.Windows.Forms.TextBox();
            this.tbConv = new System.Windows.Forms.TextBox();
            this.btSend = new System.Windows.Forms.Button();
            this.tbMsg = new System.Windows.Forms.TextBox();
            this.lbPeerList = new System.Windows.Forms.Label();
            this.lbNick = new System.Windows.Forms.Label();
            this.lbPort = new System.Windows.Forms.Label();
            this.lbConv = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // btReg
            // 
            this.btReg.Location = new System.Drawing.Point(12, 29);
            this.btReg.Name = "btReg";
            this.btReg.Size = new System.Drawing.Size(84, 29);
            this.btReg.TabIndex = 0;
            this.btReg.Text = "Register";
            this.btReg.UseVisualStyleBackColor = true;
            this.btReg.Click += new System.EventHandler(this.btReg_Click);
            // 
            // tbNick
            // 
            this.tbNick.Location = new System.Drawing.Point(102, 29);
            this.tbNick.Name = "tbNick";
            this.tbNick.Size = new System.Drawing.Size(137, 27);
            this.tbNick.TabIndex = 1;
            // 
            // tbPort
            // 
            this.tbPort.Location = new System.Drawing.Point(245, 29);
            this.tbPort.Name = "tbPort";
            this.tbPort.Size = new System.Drawing.Size(108, 27);
            this.tbPort.TabIndex = 2;
            // 
            // tbRegResult
            // 
            this.tbRegResult.Location = new System.Drawing.Point(359, 29);
            this.tbRegResult.Multiline = true;
            this.tbRegResult.Name = "tbRegResult";
            this.tbRegResult.Size = new System.Drawing.Size(152, 386);
            this.tbRegResult.TabIndex = 3;
            // 
            // tbConv
            // 
            this.tbConv.Location = new System.Drawing.Point(12, 137);
            this.tbConv.Multiline = true;
            this.tbConv.Name = "tbConv";
            this.tbConv.Size = new System.Drawing.Size(341, 278);
            this.tbConv.TabIndex = 4;
            // 
            // btSend
            // 
            this.btSend.Location = new System.Drawing.Point(12, 73);
            this.btSend.Name = "btSend";
            this.btSend.Size = new System.Drawing.Size(84, 29);
            this.btSend.TabIndex = 5;
            this.btSend.Text = "Send";
            this.btSend.UseVisualStyleBackColor = true;
            this.btSend.Click += new System.EventHandler(this.btSend_Click);
            // 
            // tbMsg
            // 
            this.tbMsg.Location = new System.Drawing.Point(102, 74);
            this.tbMsg.Name = "tbMsg";
            this.tbMsg.Size = new System.Drawing.Size(251, 27);
            this.tbMsg.TabIndex = 6;
            // 
            // lbPeerList
            // 
            this.lbPeerList.AutoSize = true;
            this.lbPeerList.Location = new System.Drawing.Point(359, 9);
            this.lbPeerList.Name = "lbPeerList";
            this.lbPeerList.Size = new System.Drawing.Size(91, 20);
            this.lbPeerList.TabIndex = 7;
            this.lbPeerList.Text = "Online Users";
            // 
            // lbNick
            // 
            this.lbNick.AutoSize = true;
            this.lbNick.Location = new System.Drawing.Point(102, 6);
            this.lbNick.Name = "lbNick";
            this.lbNick.Size = new System.Drawing.Size(75, 20);
            this.lbNick.TabIndex = 8;
            this.lbNick.Text = "Nickname";
            // 
            // lbPort
            // 
            this.lbPort.AutoSize = true;
            this.lbPort.Location = new System.Drawing.Point(245, 6);
            this.lbPort.Name = "lbPort";
            this.lbPort.Size = new System.Drawing.Size(35, 20);
            this.lbPort.TabIndex = 9;
            this.lbPort.Text = "Port";
            // 
            // lbConv
            // 
            this.lbConv.AutoSize = true;
            this.lbConv.Location = new System.Drawing.Point(12, 114);
            this.lbConv.Name = "lbConv";
            this.lbConv.Size = new System.Drawing.Size(95, 20);
            this.lbConv.TabIndex = 10;
            this.lbConv.Text = "Conversation";
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(8F, 20F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(523, 429);
            this.Controls.Add(this.lbConv);
            this.Controls.Add(this.lbPort);
            this.Controls.Add(this.lbNick);
            this.Controls.Add(this.lbPeerList);
            this.Controls.Add(this.tbMsg);
            this.Controls.Add(this.btSend);
            this.Controls.Add(this.tbConv);
            this.Controls.Add(this.tbRegResult);
            this.Controls.Add(this.tbPort);
            this.Controls.Add(this.tbNick);
            this.Controls.Add(this.btReg);
            this.Name = "Form1";
            this.Text = "Form1";
            this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.form1_Closing);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button btReg;
        private System.Windows.Forms.TextBox tbNick;
        private System.Windows.Forms.TextBox tbPort;
        private System.Windows.Forms.TextBox tbRegResult;
        private System.Windows.Forms.TextBox tbConv;
        private System.Windows.Forms.Button btSend;
        private System.Windows.Forms.TextBox tbMsg;
        private System.Windows.Forms.Label lbPeerList;
        private System.Windows.Forms.Label lbNick;
        private System.Windows.Forms.Label lbPort;
        private System.Windows.Forms.Label lbConv;
    }
}

