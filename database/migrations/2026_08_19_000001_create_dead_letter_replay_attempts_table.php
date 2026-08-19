<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('dead_letter_replay_attempts', function (Blueprint $table): void {
            $table->id();
            $table->foreignId('dead_letter_message_id')
                ->constrained('dead_letter_messages')
                ->cascadeOnDelete();
            $table->boolean('successful');
            $table->text('note')->nullable();
            $table->text('error')->nullable();
            $table->timestamp('created_at')->nullable();

            $table->index(['dead_letter_message_id', 'successful']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('dead_letter_replay_attempts');
    }
};
