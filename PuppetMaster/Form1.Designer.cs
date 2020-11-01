namespace PuppetMaster
{
    partial class Form1
    {
        /// <summary>
        ///  Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        ///  Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        ///  Required method for Designer support - do not modify
        ///  the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.upload_script = new System.Windows.Forms.Button();
            this.openFileDialog1 = new System.Windows.Forms.OpenFileDialog();
            this.next_button = new System.Windows.Forms.Button();
            this.run_button = new System.Windows.Forms.Button();
            this.scriptBox = new System.Windows.Forms.RichTextBox();
            this.newCommand = new System.Windows.Forms.TextBox();
            this.new_command_button = new System.Windows.Forms.Button();
            this.clear = new System.Windows.Forms.Button();
            this.label2 = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // upload_script
            // 
            this.upload_script.Location = new System.Drawing.Point(37, 213);
            this.upload_script.Name = "upload_script";
            this.upload_script.Size = new System.Drawing.Size(104, 30);
            this.upload_script.TabIndex = 1;
            this.upload_script.Text = "Upload script";
            this.upload_script.UseVisualStyleBackColor = true;
            this.upload_script.Click += new System.EventHandler(this.upload_script_Click);
            // 
            // openFileDialog1
            // 
            this.openFileDialog1.FileName = "openFileDialog1";
            this.openFileDialog1.FileOk += new System.ComponentModel.CancelEventHandler(this.openFileDialog1_FileOk_1);
            // 
            // next_button
            // 
            this.next_button.Location = new System.Drawing.Point(37, 122);
            this.next_button.Name = "next_button";
            this.next_button.Size = new System.Drawing.Size(104, 30);
            this.next_button.TabIndex = 2;
            this.next_button.Text = "Next Command";
            this.next_button.UseVisualStyleBackColor = true;
            this.next_button.Click += new System.EventHandler(this.next_button_Click);
            // 
            // run_button
            // 
            this.run_button.Location = new System.Drawing.Point(176, 122);
            this.run_button.Name = "run_button";
            this.run_button.Size = new System.Drawing.Size(104, 30);
            this.run_button.TabIndex = 3;
            this.run_button.Text = "Run Commands";
            this.run_button.UseVisualStyleBackColor = true;
            this.run_button.Click += new System.EventHandler(this.cont_button_Click);
            // 
            // scriptBox
            // 
            this.scriptBox.BackColor = System.Drawing.SystemColors.ButtonHighlight;
            this.scriptBox.Font = new System.Drawing.Font("Segoe UI", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point);
            this.scriptBox.Location = new System.Drawing.Point(318, 49);
            this.scriptBox.Name = "scriptBox";
            this.scriptBox.ReadOnly = true;
            this.scriptBox.Size = new System.Drawing.Size(281, 255);
            this.scriptBox.TabIndex = 4;
            this.scriptBox.Text = "";
            this.scriptBox.TextChanged += new System.EventHandler(this.scriptBox_TextChanged);
            // 
            // newCommand
            // 
            this.newCommand.Font = new System.Drawing.Font("Segoe UI", 11F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point);
            this.newCommand.Location = new System.Drawing.Point(428, 313);
            this.newCommand.Multiline = true;
            this.newCommand.Name = "newCommand";
            this.newCommand.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
            this.newCommand.Size = new System.Drawing.Size(171, 30);
            this.newCommand.TabIndex = 5;
            this.newCommand.TextChanged += new System.EventHandler(this.textBox1_TextChanged);
            // 
            // new_command_button
            // 
            this.new_command_button.Location = new System.Drawing.Point(318, 313);
            this.new_command_button.Name = "new_command_button";
            this.new_command_button.Size = new System.Drawing.Size(104, 30);
            this.new_command_button.TabIndex = 6;
            this.new_command_button.Text = "New Command";
            this.new_command_button.UseVisualStyleBackColor = true;
            this.new_command_button.Click += new System.EventHandler(this.new_command_button_Click);
            // 
            // clear
            // 
            this.clear.Location = new System.Drawing.Point(176, 214);
            this.clear.Name = "clear";
            this.clear.Size = new System.Drawing.Size(104, 30);
            this.clear.TabIndex = 7;
            this.clear.Text = "Clear";
            this.clear.UseVisualStyleBackColor = true;
            this.clear.Click += new System.EventHandler(this.clear_Click);
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Font = new System.Drawing.Font("Segoe UI", 11F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point);
            this.label2.Location = new System.Drawing.Point(393, 7);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(125, 20);
            this.label2.TabIndex = 9;
            this.label2.Text = "Command Queue";
            this.label2.Click += new System.EventHandler(this.label2_Click);
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Font = new System.Drawing.Font("Times New Roman", 20F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point);
            this.label1.Location = new System.Drawing.Point(69, 26);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(173, 31);
            this.label1.TabIndex = 10;
            this.label1.Text = "Puppet Master";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Font = new System.Drawing.Font("Segoe UI", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point);
            this.label3.Location = new System.Drawing.Point(385, 27);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(146, 13);
            this.label3.TabIndex = 11;
            this.label3.Text = "(  next command in             )";
            this.label3.Click += new System.EventHandler(this.label3_Click);
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Font = new System.Drawing.Font("Segoe UI", 8F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point);
            this.label4.ForeColor = System.Drawing.Color.Green;
            this.label4.Location = new System.Drawing.Point(487, 27);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(37, 13);
            this.label4.TabIndex = 12;
            this.label4.Text = "green";
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 15F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.BackColor = System.Drawing.SystemColors.Control;
            this.ClientSize = new System.Drawing.Size(612, 371);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.clear);
            this.Controls.Add(this.new_command_button);
            this.Controls.Add(this.newCommand);
            this.Controls.Add(this.scriptBox);
            this.Controls.Add(this.run_button);
            this.Controls.Add(this.next_button);
            this.Controls.Add(this.upload_script);
            this.Name = "Form1";
            this.Text = " Puppet Master";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion
        private System.Windows.Forms.Button upload_script;
        private System.Windows.Forms.OpenFileDialog openFileDialog1;
        private System.Windows.Forms.Button next_button;
        private System.Windows.Forms.Button run_button;
        private System.Windows.Forms.RichTextBox scriptBox;
        private System.Windows.Forms.TextBox newCommand;
        private System.Windows.Forms.Button new_command_button;
        private System.Windows.Forms.Button clear;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Label label4;
    }
}

